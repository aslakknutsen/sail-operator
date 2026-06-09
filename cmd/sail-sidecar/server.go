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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	v1 "github.com/istio-ecosystem/sail-operator/api/v1"
	"github.com/istio-ecosystem/sail-operator/pkg/install"
	"github.com/google/uuid"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	pb "github.com/istio-ecosystem/sail-operator/api/sidecar/v1"
)

const olmCallbackTimeout = 30 * time.Second

// SailLibraryService implements the SailLibrary gRPC service.
type SailLibraryService struct {
	pb.UnimplementedSailLibraryServer

	lib      *install.Library
	notifyCh <-chan struct{}

	// OLM callback coordination. A single session is expected at a time.
	mu          sync.Mutex
	olmReqCh    chan *pb.OverwriteOLMCRDRequest
	olmRespCh   chan *pb.OverwriteOLMCRDResponse
	sessionLive bool
}

func NewSailLibraryService(lib *install.Library, notifyCh <-chan struct{}) *SailLibraryService {
	return &SailLibraryService{
		lib:      lib,
		notifyCh: notifyCh,
	}
}

func (s *SailLibraryService) GetStatus(_ context.Context, _ *pb.GetStatusRequest) (*pb.StatusResponse, error) {
	if s.lib == nil {
		return &pb.StatusResponse{}, nil
	}
	return statusToProto(s.lib.Status()), nil
}

func (s *SailLibraryService) Session(stream pb.SailLibrary_SessionServer) error {
	log := ctrl.Log.WithName("session")

	s.mu.Lock()
	if s.sessionLive {
		s.mu.Unlock()
		return fmt.Errorf("another session is already active")
	}
	s.olmReqCh = make(chan *pb.OverwriteOLMCRDRequest, 1)
	s.olmRespCh = make(chan *pb.OverwriteOLMCRDResponse, 1)
	s.sessionLive = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.sessionLive = false
		s.mu.Unlock()
	}()

	ctx := stream.Context()

	// sendCh serializes writes to the stream from multiple goroutines.
	sendCh := make(chan *pb.ServerMessage, 16)

	// Writer goroutine: sends messages to the client.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-sendCh:
				if !ok {
					return
				}
				if err := stream.Send(msg); err != nil {
					log.Error(err, "failed to send message")
					return
				}
			}
		}
	}()

	// Status notification goroutine: forwards library status updates.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-s.notifyCh:
				if !ok {
					return
				}
				var st install.Status
				if s.lib != nil {
					st = s.lib.Status()
				}
				status := statusToProto(st)
				select {
				case sendCh <- &pb.ServerMessage{Msg: &pb.ServerMessage_StatusUpdate{StatusUpdate: &pb.StatusUpdate{Status: status}}}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// OLM callback forwarding goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case req, ok := <-s.olmReqCh:
				if !ok {
					return
				}
				select {
				case sendCh <- &pb.ServerMessage{Msg: &pb.ServerMessage_OlmCrdRequest{OlmCrdRequest: req}}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Reader loop: processes incoming messages from the client.
	for {
		in, err := stream.Recv()
		if err != nil {
			log.V(2).Info("session ended", "error", err)
			break
		}

		switch msg := in.Msg.(type) {
		case *pb.ClientMessage_Apply:
			resp := s.handleApply(ctx, msg.Apply)
			select {
			case sendCh <- &pb.ServerMessage{Msg: &pb.ServerMessage_ApplyResponse{ApplyResponse: resp}}:
			case <-ctx.Done():
			}

		case *pb.ClientMessage_Uninstall:
			resp := s.handleUninstall(ctx, msg.Uninstall)
			select {
			case sendCh <- &pb.ServerMessage{Msg: &pb.ServerMessage_UninstallResponse{UninstallResponse: resp}}:
			case <-ctx.Done():
			}

		case *pb.ClientMessage_Enqueue:
			if s.lib != nil {
				s.lib.Enqueue()
			}

		case *pb.ClientMessage_OlmCrdResponse:
			select {
			case s.olmRespCh <- msg.OlmCrdResponse:
			default:
				log.Info("OLM response dropped; no pending request")
			}

		case *pb.ClientMessage_Stop:
			if s.lib != nil {
				s.lib.Stop()
			}
		}
	}

	close(sendCh)
	wg.Wait()
	return nil
}

func (s *SailLibraryService) handleApply(ctx context.Context, req *pb.ApplyRequest) *pb.ApplyResponse {
	log := ctrl.Log.WithName("session").WithValues("op", "apply", "namespace", req.Namespace, "version", req.Version)
	var values *v1.Values
	if len(req.ValuesJson) > 0 {
		values = &v1.Values{}
		if err := json.Unmarshal(req.ValuesJson, values); err != nil {
			return &pb.ApplyResponse{Error: fmt.Sprintf("failed to unmarshal values: %v", err)}
		}
	}

	opts := install.Options{
		Namespace:              req.Namespace,
		Version:                req.Version,
		Revision:               req.Revision,
		Values:                 values,
		ManageCRDs:             req.ManageCrds,
		IncludeAllCRDs:         req.IncludeAllCrds,
		OverwriteOLMManagedCRD: s.makeOLMCallback(ctx),
	}

	if s.lib == nil {
		return &pb.ApplyResponse{Error: "library not initialized"}
	}
	log.V(1).Info("applying")
	if err := s.lib.Apply(opts); err != nil {
		log.Error(err, "apply failed")
		return &pb.ApplyResponse{Error: err.Error()}
	}
	log.V(1).Info("apply queued")
	return &pb.ApplyResponse{}
}

func (s *SailLibraryService) handleUninstall(ctx context.Context, req *pb.UninstallRequest) *pb.UninstallResponse {
	log := ctrl.Log.WithName("session").WithValues("op", "uninstall", "namespace", req.Namespace, "revision", req.Revision)
	if s.lib == nil {
		return &pb.UninstallResponse{Error: "library not initialized"}
	}
	log.V(1).Info("uninstalling")
	if err := s.lib.Uninstall(ctx, req.Namespace, req.Revision); err != nil {
		return &pb.UninstallResponse{Error: err.Error()}
	}
	return &pb.UninstallResponse{}
}

// makeOLMCallback returns the OverwriteOLMManagedCRDFunc that bridges the
// library callback to the gRPC stream. When the library encounters an
// OLM-managed CRD, this function sends a request to the client and blocks
// until the client responds or the timeout expires.
func (s *SailLibraryService) makeOLMCallback(ctx context.Context) install.OverwriteOLMManagedCRDFunc {
	return func(_ context.Context, crd *apiextensionsv1.CustomResourceDefinition) bool {
		log := ctrl.Log.WithName("olm-callback")

		reqID := uuid.New().String()
		req := &pb.OverwriteOLMCRDRequest{
			RequestId: reqID,
			CrdName:   crd.Name,
			CrdLabels: crd.Labels,
		}

		select {
		case s.olmReqCh <- req:
		case <-ctx.Done():
			return false
		}

		select {
		case resp := <-s.olmRespCh:
			if resp.RequestId != reqID {
				log.Info("OLM response ID mismatch", "expected", reqID, "got", resp.RequestId)
				return false
			}
			return resp.Overwrite
		case <-time.After(olmCallbackTimeout):
			log.Info("OLM callback timed out", "crd", crd.Name)
			return false
		case <-ctx.Done():
			return false
		}
	}
}

func statusToProto(s install.Status) *pb.StatusResponse {
	resp := &pb.StatusResponse{
		Generation: s.Generation,
		CrdState:   string(s.CRDState),
		CrdMessage: s.CRDMessage,
		Installed:  s.Installed,
		Version:    s.Version,
	}
	if s.Error != nil {
		resp.Error = s.Error.Error()
	}
	for _, c := range s.CRDs {
		resp.Crds = append(resp.Crds, &pb.CRDInfo{
			Name:    c.Name,
			Managed: c.Managed,
			Ready:   c.Ready,
		})
	}
	return resp
}
