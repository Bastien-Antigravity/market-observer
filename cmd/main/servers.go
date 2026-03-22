package main

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"market-observer/src/config"
	datasource "market-observer/src/data_source"
	pb "market-observer/src/grpc_control"
	"market-observer/src/interfaces"
	"market-observer/src/logger"
	"market-observer/src/rest"
)

// startServers boots up the gRPC and REST proxy bounds
func startServers(
	cfg *config.Config,
	configPath string,
	appLogger *logger.Logger,
	multiSource *datasource.MultiSourceManager,
	networkManage interfaces.INetworkManager,
) {
	// Start gRPC Control Server
	go func() {
		port := cfg.GrpcPort
		if port == 0 {
			port = 50051 // Default fallback
		}
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			appLogger.Critical("failed to listen for gRPC: %v", err)
			return
		}
		grpcServer := grpc.NewServer()
		grpcLogger := logger.NewLogger(cfg.MConfig, "ControlService")
		controlService := pb.NewControlService(cfg, multiSource, configPath, grpcLogger, networkManage)
		pb.RegisterMarketObserverControlServer(grpcServer, controlService)

		appLogger.Info("Starting gRPC Control Server on :%d", port)
		if err := grpcServer.Serve(lis); err != nil {
			appLogger.Critical("failed to serve gRPC: %v", err)
		}
	}()

	// REST Control Server (gRPC Proxy)
	go func() {
		port := cfg.GrpcPort
		if port == 0 {
			port = 50051 // Default fallback
		}

		time.Sleep(1 * time.Second)

		conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			appLogger.Error("Failed to dial gRPC for REST gateway: %v", err)
			return
		}

		client := pb.NewMarketObserverControlClient(conn)
		restLogger := logger.NewLogger(cfg.MConfig, "RestHandler")
		restHandler := rest.NewRestHandler(client, restLogger)

		restMux := http.NewServeMux()
		restHandler.RegisterEndpoints(restMux)

		appLogger.Info("Starting REST Control Server on :8081")
		if err := http.ListenAndServe(":8081", restMux); err != nil {
			appLogger.Error("REST Control Server failed: %v", err)
		}
	}()
}
