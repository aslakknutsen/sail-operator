// Copyright Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/istio-ecosystem/sail-operator/chart"
	"github.com/istio-ecosystem/sail-operator/pkg/install"
	"github.com/istio-ecosystem/sail-operator/resources"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	pb "github.com/istio-ecosystem/sail-operator/api/sidecar/v1"
)

func main() {
	socketPath := flag.String("socket", "/var/run/sail/sail.sock", "Unix domain socket path")
	flag.Parse()

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	logf.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("sail-sidecar")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	cfg, err := ctrl.GetConfig()
	if err != nil {
		log.Error(err, "unable to get kubeconfig")
		os.Exit(1)
	}

	lib, err := install.New(cfg, resources.FS, chart.CRDsFS)
	if err != nil {
		log.Error(err, "failed to create install library")
		os.Exit(1)
	}

	notifyCh, err := lib.Start(ctx)
	if err != nil {
		log.Error(err, "failed to start install library")
		os.Exit(1)
	}

	if err := os.MkdirAll(filepath.Dir(*socketPath), 0o755); err != nil {
		log.Error(err, "failed to create socket directory")
		os.Exit(1)
	}
	_ = os.Remove(*socketPath)

	lis, err := net.Listen("unix", *socketPath)
	if err != nil {
		log.Error(err, "failed to listen on socket", "path", *socketPath)
		os.Exit(1)
	}
	log.Info("listening", "socket", *socketPath)

	grpcServer := grpc.NewServer()
	svc := NewSailLibraryService(lib, notifyCh)
	pb.RegisterSailLibraryServer(grpcServer, svc)

	healthSvc := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSvc)
	healthSvc.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		healthSvc.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		lib.Stop()
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Error(err, "gRPC server exited with error")
		os.Exit(1)
	}
}
