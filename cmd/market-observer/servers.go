package main

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"market-observer/src/config"
	datasource "market-observer/src/data_source"
	pb "market-observer/src/grpc_control"
	"market-observer/src/interfaces"
	"market-observer/src/rest"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// -----------------------------------------------------------------------------

// startServers orchestrates the startup of all server components
func startServers(
	srv interfaces.IDataExchanger,
	multiSource *datasource.MultiSourceManager,
	config *config.Config,
	configPath string,
	appLogger interfaces.Logger,
	networkManager interfaces.INetworkManager,
) {
	// 1. FastAPIServer
	go func() {
		if err := srv.Start(); err != nil {
			appLogger.Error(fmt.Sprintf("Server failed: %v", err))
		}
	}()

	// 2. gRPC Control Server
	go func() {
		port := config.GrpcPort
		if port == 0 {
			port = 50051 // Default fallback
		}
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			appLogger.Critical(fmt.Sprintf("failed to listen for gRPC: %v", err))
			return // or os.Exit
		}
		grpcServer := grpc.NewServer()
		controlService := pb.NewControlService(config, multiSource, configPath, appLogger, networkManager)
		pb.RegisterMarketObserverControlServer(grpcServer, controlService)

		appLogger.Info(fmt.Sprintf("Starting gRPC Control Server on :%d", port))
		if err := grpcServer.Serve(lis); err != nil {
			appLogger.Critical(fmt.Sprintf("failed to serve gRPC: %v", err))
		}
	}()

	// 4. REST Control Server (gRPC Proxy)
	go func() {
		port := config.GrpcPort
		if port == 0 {
			port = 50051 // Default fallback
		}

		// Wait briefly to ensure gRPC server is listening
		time.Sleep(1 * time.Second)

		// Create gRPC client
		conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			appLogger.Error(fmt.Sprintf("Failed to dial gRPC for REST gateway: %v", err))
			return
		}
		// Connection remains open for the lifetime of the server

		client := pb.NewMarketObserverControlClient(conn)
		restHandler := rest.NewRestHandler(client, appLogger)

		restMux := http.NewServeMux()
		restHandler.RegisterEndpoints(restMux)

		appLogger.Info("Starting REST Control Server on :8081")
		if err := http.ListenAndServe(":8081", restMux); err != nil {
			appLogger.Error(fmt.Sprintf("REST Control Server failed: %v", err))
		}
	}()
}
