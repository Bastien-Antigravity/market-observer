package helpers

import (
	"fmt"
	"io"
	"market-observer/src/logger"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

// -----------------------------------------------------------------------------

type ProxyManager struct {
	proxies    []string
	userAgents []string
	index      int
	mu         sync.Mutex
	logger     *logger.Logger
	httpClient *http.Client
}

// -----------------------------------------------------------------------------

func NewProxyManager(proxies []string) *ProxyManager {
	// Validate and format proxies on init
	var validProxies []string
	for _, p := range proxies {
		if ValidateProxy(p) {
			validProxies = append(validProxies, FormatProxy(p))
		}
	}

	pm := &ProxyManager{
		proxies: validProxies,
		logger:  logger.NewLogger(nil, "ProxyManager"),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		userAgents: []string{
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
			"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
			"Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
			"Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
			"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0",
		},
	}
	rand.Seed(time.Now().UnixNano())
	return pm
}

// -----------------------------------------------------------------------------

func (pm *ProxyManager) GetCurrentProxy() (string, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if len(pm.proxies) == 0 {
		return "", nil
	}
	return pm.proxies[pm.index], nil
}

// -----------------------------------------------------------------------------

func (pm *ProxyManager) RotateProxy() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if len(pm.proxies) <= 1 {
		return
	}

	pm.index = (pm.index + 1) % len(pm.proxies)
	pm.logger.Info("Rotating proxy to: %s", pm.proxies[pm.index])
}

// -----------------------------------------------------------------------------

func (pm *ProxyManager) GetUserAgent() string {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if len(pm.userAgents) == 0 {
		return "Mozilla/5.0 (Go-http-client/1.1)"
	}
	return pm.userAgents[rand.Intn(len(pm.userAgents))]
}

// -----------------------------------------------------------------------------

// RefreshProxies scrapes SSLProxies.org for new proxies
func (pm *ProxyManager) RefreshProxies() (int, error) {
	pm.logger.Info("Refresing proxies from https://www.sslproxies.org/...")

	req, err := http.NewRequest("GET", "https://www.sslproxies.org/", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", pm.GetUserAgent())

	resp, err := pm.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	// Regex to find IP:Port in table cells
	// Expected format: <tr><td>1.2.3.4</td><td>8080</td>...
	re := regexp.MustCompile(`<tr><td>(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})</td><td>(\d+)</td>`)
	matches := re.FindAllStringSubmatch(string(body), -1)

	var newProxies []string
	for _, match := range matches {
		if len(match) == 3 {
			ip := match[1]
			port := match[2]
			proxy := fmt.Sprintf("http://%s:%s", ip, port)
			newProxies = append(newProxies, proxy)
		}
	}

	if len(newProxies) == 0 {
		return 0, fmt.Errorf("no proxies found on page")
	}

	// Shuffle
	rand.Shuffle(len(newProxies), func(i, j int) {
		newProxies[i], newProxies[j] = newProxies[j], newProxies[i]
	})

	// Limit to top 50
	if len(newProxies) > 50 {
		newProxies = newProxies[:50]
	}

	pm.mu.Lock()
	pm.proxies = newProxies
	pm.index = 0
	pm.mu.Unlock()

	pm.logger.Info("Found and updated %d proxies", len(newProxies))
	return len(newProxies), nil
}

// -----------------------------------------------------------------------------

func (pm *ProxyManager) ValidateProxy(proxyStr string) bool {
	return ValidateProxy(proxyStr)
}

// -----------------------------------------------------------------------------

func (pm *ProxyManager) HasProxies() bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return len(pm.proxies) > 0
}

// -----------------------------------------------------------------------------

// ValidateProxy checks if a proxy string is roughly valid.
func ValidateProxy(proxyStr string) bool {
	u, err := url.Parse(proxyStr)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https" || u.Scheme == "socks5" || u.Scheme == "") // Allow missing scheme for FormatProxy to fix
}

// -----------------------------------------------------------------------------

// FormatProxy ensures the proxy has a scheme.
func FormatProxy(proxyStr string) string {
	if !strings.Contains(proxyStr, "://") {
		return "http://" + proxyStr
	}
	return proxyStr
}
