package main

import (
	"fmt"
	"market-observer/src/config"
	datasource "market-observer/src/data_source"
	pb "market-observer/src/grpc_control"
	"market-observer/src/interfaces"
	"market-observer/src/logger"
	"net"
	"net/http"
	"os"

	"google.golang.org/grpc"
)

// -----------------------------------------------------------------------------

// startServers orchestrates the startup of all server components
func startServers(
	srv interfaces.IDataExchanger,
	multiSource *datasource.MultiSourceManager,
	config *config.Config,
	configPath string,
	appLogger *logger.Logger,
	networkManager interfaces.INetworkManager,
) {

	// 1. FastAPIServer
	go func() {
		if err := srv.Start(); err != nil {
			appLogger.Error("Server failed: %v", err)
		}
	}()

	// 2. Static Web Server on 8080
	go webserverTest(appLogger)

	// 3. gRPC Control Server
	go func() {
		port := config.GrpcPort
		if port == 0 {
			port = 50051 // Default fallback
		}
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			appLogger.Critical("failed to listen for gRPC: %v", err)
			return // or os.Exit
		}
		grpcServer := grpc.NewServer()
		grpcLogger := logger.NewLogger(config, "ControlService")
		controlService := pb.NewControlService(config, multiSource, configPath, grpcLogger, networkManager)
		pb.RegisterMarketObserverControlServer(grpcServer, controlService)

		appLogger.Info("Starting gRPC Control Server on :%d", port)
		if err := grpcServer.Serve(lis); err != nil {
			appLogger.Critical("failed to serve gRPC: %v", err)
		}
	}()
}

// -----------------------------------------------------------------------------

// webserverTest runs a simple static file server for testing
func webserverTest(logger *logger.Logger) {
	staticMux := http.NewServeMux()
	staticMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		file := "market-observer.html"
		if _, err := os.Stat(file); os.IsNotExist(err) {
			if _, err := os.Stat("src/server/" + file); err == nil {
				file = "src/server/" + file
			} else if _, err := os.Stat("../src/server/" + file); err == nil {
				file = "../src/server/" + file
			}
		}
		http.ServeFile(w, r, file)
	})
	// Serve static assets
	// Try to find static dir
	staticDir := "static"
	if _, err := os.Stat(staticDir); os.IsNotExist(err) {
		if _, err := os.Stat("src/server/static"); err == nil {
			staticDir = "src/server/static"
		} else if _, err := os.Stat("../src/server/static"); err == nil {
			staticDir = "../src/server/static"
		}
	}

	staticMux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir(staticDir))))

	logger.Info("Starting Static Web Server on http://127.0.0.1:8080")
	if err := http.ListenAndServe(":8080", staticMux); err != nil {
		logger.Error("Static Web Server failed: %v", err)
	}
}
