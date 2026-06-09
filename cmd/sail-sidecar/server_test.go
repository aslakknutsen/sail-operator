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
	"net"
	"testing"
	"time"

	pb "github.com/istio-ecosystem/sail-operator/api/sidecar/v1"
	"github.com/istio-ecosystem/sail-operator/pkg/install"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startTestServer(t *testing.T, lib *install.Library, notifyCh <-chan struct{}) (pb.SailLibraryClient, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	svc := NewSailLibraryService(lib, notifyCh)
	pb.RegisterSailLibraryServer(srv, svc)

	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	client := pb.NewSailLibraryClient(conn)

	cleanup := func() {
		conn.Close()
		srv.GracefulStop()
		lis.Close()
	}
	return client, cleanup
}

func TestGetStatus_EmptyLibrary(t *testing.T) {
	// With a nil library we can only test that GetStatus doesn't panic.
	// A real integration test would use an actual library instance.
	notifyCh := make(chan struct{})
	client, cleanup := startTestServer(t, nil, notifyCh)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if resp.Installed {
		t.Error("expected Installed=false for nil library")
	}
}

func TestSession_ApplyWithNilLibrary(t *testing.T) {
	notifyCh := make(chan struct{})
	client, cleanup := startTestServer(t, nil, notifyCh)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Session(ctx)
	if err != nil {
		t.Fatalf("Session failed: %v", err)
	}

	err = stream.Send(&pb.ClientMessage{
		Msg: &pb.ClientMessage_Apply{
			Apply: &pb.ApplyRequest{
				Namespace: "istio-system",
				Version:   "v1.24.3",
				ValuesJson: []byte(`{}`),
			},
		},
	})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv failed: %v", err)
	}

	applyResp := resp.GetApplyResponse()
	if applyResp == nil {
		t.Fatal("expected ApplyResponse")
	}
	// With nil library, Apply will fail
	if applyResp.Error == "" {
		t.Error("expected error with nil library")
	}
}

func TestSession_StatusStream(t *testing.T) {
	notifyCh := make(chan struct{}, 1)
	client, cleanup := startTestServer(t, nil, notifyCh)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Session(ctx)
	if err != nil {
		t.Fatalf("Session failed: %v", err)
	}

	// Trigger a status notification
	notifyCh <- struct{}{}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv failed: %v", err)
	}

	statusUpdate := resp.GetStatusUpdate()
	if statusUpdate == nil {
		t.Fatal("expected StatusUpdate")
	}
	if statusUpdate.Status == nil {
		t.Fatal("expected non-nil status")
	}
}

func TestSession_OLMCallback(t *testing.T) {
	notifyCh := make(chan struct{})
	svc := NewSailLibraryService(nil, notifyCh)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterSailLibraryServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	defer func() { srv.GracefulStop(); lis.Close() }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := pb.NewSailLibraryClient(conn).Session(ctx)
	if err != nil {
		t.Fatalf("Session failed: %v", err)
	}

	// Simulate the library triggering an OLM callback by writing to olmReqCh.
	// We need a session to be established first, so wait a moment.
	time.Sleep(100 * time.Millisecond)

	go func() {
		svc.olmReqCh <- &pb.OverwriteOLMCRDRequest{
			RequestId: "test-req-1",
			CrdName:   "virtualservices.networking.istio.io",
			CrdLabels: map[string]string{"olm.managed": "true"},
		}
	}()

	// Client should receive the OLM request
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv failed: %v", err)
	}
	olmReq := resp.GetOlmCrdRequest()
	if olmReq == nil {
		t.Fatalf("expected OverwriteOLMCRDRequest, got %T", resp.Msg)
	}
	if olmReq.RequestId != "test-req-1" {
		t.Errorf("expected request ID test-req-1, got %s", olmReq.RequestId)
	}
	if olmReq.CrdName != "virtualservices.networking.istio.io" {
		t.Errorf("unexpected CRD name: %s", olmReq.CrdName)
	}

	// Client responds
	err = stream.Send(&pb.ClientMessage{
		Msg: &pb.ClientMessage_OlmCrdResponse{
			OlmCrdResponse: &pb.OverwriteOLMCRDResponse{
				RequestId: "test-req-1",
				Overwrite: true,
			},
		},
	})
	if err != nil {
		t.Fatalf("Send OLM response failed: %v", err)
	}

	// Verify the response was delivered
	select {
	case resp := <-svc.olmRespCh:
		if resp.RequestId != "test-req-1" {
			t.Errorf("expected request ID test-req-1, got %s", resp.RequestId)
		}
		if !resp.Overwrite {
			t.Error("expected overwrite=true")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for OLM response")
	}
}
