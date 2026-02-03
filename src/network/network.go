package network

import (
	"crypto/tls"
	"fmt"
	"io"
	"market-observer/src/helpers"
	"market-observer/src/interfaces"
	"market-observer/src/logger"
	"market-observer/src/models"
	"net/http"
	"net/url"
	"time"
)

type AsyncNetworkManager struct {
	Config       *models.MConfig
	ProxyManager interfaces.IProxyManager
	Client       *http.Client
	Logger       *logger.Logger
}

// -----------------------------------------------------------------------------

func NewAsyncNetworkManager(cfg *models.MConfig, log *logger.Logger) *AsyncNetworkManager {
	var proxies []string
	if cfg.Network.Enabled {
		proxies = cfg.Network.Proxies
	}

	nm := &AsyncNetworkManager{
		Config:       cfg,
		ProxyManager: helpers.NewProxyManager(proxies),
		Logger:       log,
	}
	nm.Client = nm.createClient()
	return nm
}

// -----------------------------------------------------------------------------

func (nm *AsyncNetworkManager) createClient() *http.Client {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	if nm.ProxyManager.HasProxies() {
		proxyStr, err := nm.ProxyManager.GetCurrentProxy()
		if err == nil && proxyStr != "" {
			proxyURL, err := url.Parse(proxyStr)
			if err == nil {
				transport.Proxy = http.ProxyURL(proxyURL)
			}
		}
	}

	return &http.Client{
		Transport: transport,
		Timeout:   time.Duration(nm.Config.Network.RequestTimeout) * time.Second,
	}
}

// -----------------------------------------------------------------------------

func (nm *AsyncNetworkManager) rotateProxy() {
	if !nm.ProxyManager.HasProxies() {
		return
	}

	nm.ProxyManager.RotateProxy()
	nm.Client = nm.createClient()
}

// -----------------------------------------------------------------------------

// Get performs a GET request with retries and proxy rotation.
func (nm *AsyncNetworkManager) Get(urlStr string, params map[string]string) ([]byte, error) {
	reqUrl, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	q := reqUrl.Query()
	for k, v := range params {
		q.Add(k, v)
	}
	reqUrl.RawQuery = q.Encode()

	finalUrl := reqUrl.String()

	maxRetries := nm.Config.Network.MaxRetries
	var lastErr error

	for i := 0; i <= maxRetries; i++ {
		if i > 0 {
			time.Sleep(time.Duration(i*i) * time.Second) // Exponential backoff
			nm.rotateProxy()
		}

		req, err := http.NewRequest("GET", finalUrl, nil)
		if err != nil {
			return nil, err
		}

		// Use dynamic User-Agent
		req.Header.Set("User-Agent", nm.ProxyManager.GetUserAgent())

		resp, err := nm.Client.Do(req)
		if err != nil {
			lastErr = err
			nm.Logger.Info("Request failed (attempt %d/%d): %v", i+1, maxRetries+1, err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == 429 || resp.StatusCode == 403 {
			lastErr = fmt.Errorf("blocked (status %d)", resp.StatusCode)
			nm.Logger.Info("Request blocked (%d). Rotating proxy.", resp.StatusCode)

			// If we are getting blocked repeatedly, try to refresh proxies
			if i == maxRetries-1 && nm.Config.Network.Enabled {
				nm.Logger.Warning("Repeated blocks. Attempting to scrape new proxies...")
				count, refreshErr := nm.ProxyManager.RefreshProxies()
				if refreshErr == nil && count > 0 {
					nm.Logger.Info("Refreshed %d proxies. Retrying...", count)
					nm.rotateProxy()
				} else {
					nm.Logger.Error("Failed to refresh proxies: %v", refreshErr)
				}
			}
			continue
		}

		if resp.StatusCode != 200 {
			lastErr = fmt.Errorf("bad status: %d", resp.StatusCode)
			nm.Logger.Info("Bad status %d", resp.StatusCode)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			lastErr = err
			continue
		}

		return body, nil
	}

	// Try one last desperate refresh if enabled
	if nm.Config.Network.Enabled {
		nm.ProxyManager.RefreshProxies()
	}

	return nil, fmt.Errorf("max retries exceeded: %v", lastErr)
}
