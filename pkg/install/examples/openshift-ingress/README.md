# OpenShift Ingress Integration Example

This example shows how the OpenShift Ingress Operator can use the Sail Library
to install Istio as a Gateway API controller.

## Overview

The OpenShift Ingress Operator manages Istio as the Gateway API implementation
for OpenShift. Instead of creating Istio CRs directly, it can use the Sail
Library's `gateway-api` preset with OpenShift-specific overrides.

## Usage

```go
package gatewayclass

import (
    "context"

    "github.com/istio-ecosystem/sail-operator/pkg/install"
    "github.com/istio-ecosystem/sail-operator/resources"
    v1 "github.com/istio-ecosystem/sail-operator/api/v1"
    "github.com/istio-ecosystem/sail-operator/pkg/config"
    "k8s.io/client-go/rest"
    "k8s.io/utils/ptr"
)

const (
    // OpenShift-specific constants
    OpenShiftDefaultGatewayClassName      = "openshift-default"
    OpenShiftGatewayClassControllerName   = "openshift.io/gateway-controller"
    DefaultOperandNamespace               = "openshift-ingress"
    OperatorNamespace                     = "openshift-ingress-operator"
    OpenShiftGatewayCARootCertName        = "openshift-gateway-ca-root-cert"
    SystemClusterCriticalPriorityClass    = "system-cluster-critical"
    WorkloadPartitioningAnnotationKey     = "target.workload.openshift.io/management"
    WorkloadPartitioningAnnotationValue   = `{"effect": "PreferredDuringScheduling"}`
)

// InstallIstio installs Istio configured for Gateway API using the Sail Library
func InstallIstio(ctx context.Context, kubeConfig *rest.Config, istioVersion string) error {
    // Create installer with OpenShift configuration
    installer, err := install.NewInstaller(install.Options{
        KubeConfig:        kubeConfig,
        ResourceFS:        resources.FS,        // Use embedded resources
        Platform:          config.PlatformOpenShift,
        DefaultProfile:    "openshift",
        OperatorNamespace: OperatorNamespace,
    })
    if err != nil {
        return err
    }

    // Install using the gateway-api preset with OpenShift-specific overrides
    return installer.InstallWithOverrides(ctx, install.PresetGatewayAPI, &install.Overrides{
        Namespace: DefaultOperandNamespace,
        Version:   istioVersion,
        Values:    openshiftValues(),
    })
}

// openshiftValues returns the OpenShift-specific value overrides
func openshiftValues() *v1.Values {
    return &v1.Values{
        Global: &v1.GlobalConfig{
            // Use system-cluster-critical priority for OpenShift system workloads
            PriorityClassName: ptr.To(SystemClusterCriticalPriorityClass),
            // OpenShift-specific CA trust bundle
            TrustBundleName: ptr.To(OpenShiftGatewayCARootCertName),
        },
        Pilot: &v1.PilotConfig{
            Env: map[string]string{
                // OpenShift-specific gatewayclass name
                "PILOT_GATEWAY_API_DEFAULT_GATEWAYCLASS_NAME": OpenShiftDefaultGatewayClassName,
                // OpenShift-specific controller name
                "PILOT_GATEWAY_API_CONTROLLER_NAME": OpenShiftGatewayClassControllerName,
            },
            // Workload partitioning for OpenShift management workloads
            PodAnnotations: map[string]string{
                WorkloadPartitioningAnnotationKey: WorkloadPartitioningAnnotationValue,
            },
        },
    }
}

// UninstallIstio removes the Istio installation
func UninstallIstio(ctx context.Context, kubeConfig *rest.Config) error {
    installer, err := install.NewInstaller(install.Options{
        KubeConfig:        kubeConfig,
        ResourceFS:        resources.FS,
        Platform:          config.PlatformOpenShift,
        OperatorNamespace: OperatorNamespace,
    })
    if err != nil {
        return err
    }

    return installer.UninstallWithOverrides(ctx, install.PresetGatewayAPI, &install.Overrides{
        Namespace: DefaultOperandNamespace,
    })
}
```

## What the Preset Provides

The `gateway-api` preset configures Istio with:

| Setting | Value | Purpose |
|---------|-------|---------|
| `PILOT_ENABLE_GATEWAY_API` | `true` | Enable Gateway API support |
| `PILOT_ENABLE_GATEWAY_API_STATUS` | `true` | Update Gateway API resource status |
| `PILOT_ENABLE_GATEWAY_API_DEPLOYMENT_CONTROLLER` | `true` | Auto-create Envoy deployments for Gateways |
| `PILOT_ENABLE_GATEWAY_API_GATEWAYCLASS_CONTROLLER` | `false` | Let cluster-admin manage GatewayClass |
| `PILOT_ENABLE_ALPHA_GATEWAY_API` | `false` | Disable experimental features |
| `PILOT_MULTI_NETWORK_DISCOVER_GATEWAY_API` | `false` | Disable multi-network discovery |
| `ENABLE_GATEWAY_API_MANUAL_DEPLOYMENT` | `false` | Only automated deployments |
| `PILOT_ENABLE_GATEWAY_API_CA_CERT_ONLY` | `true` | CA ConfigMap only where needed |
| `PILOT_ENABLE_GATEWAY_API_COPY_LABELS_ANNOTATIONS` | `false` | Don't copy gateway labels |
| `meshConfig.ingressControllerMode` | `OFF` | Disable Ingress controller |
| `meshConfig.accessLogFile` | `/dev/stdout` | Access logging enabled |
| `sidecarInjectorWebhook.enableNamespacesByDefault` | `false` | No auto-injection |

## OpenShift-Specific Overrides

These must be provided via `Overrides.Values`:

| Setting | Example Value | Purpose |
|---------|---------------|---------|
| `PILOT_GATEWAY_API_DEFAULT_GATEWAYCLASS_NAME` | `openshift-default` | OpenShift GatewayClass name |
| `PILOT_GATEWAY_API_CONTROLLER_NAME` | `openshift.io/gateway-controller` | OpenShift controller name |
| `global.priorityClassName` | `system-cluster-critical` | System workload priority |
| `global.trustBundleName` | `openshift-gateway-ca-root-cert` | OpenShift CA cert name |
| `pilot.podAnnotations` | workload partitioning | OpenShift management scheduling |

## Dependencies

Add the Sail Operator as a Go module dependency:

```bash
go get github.com/istio-ecosystem/sail-operator@latest
```

## Notes

1. **Resource Embedding**: The example uses `resources.FS` which contains embedded
   Helm charts. This increases binary size but removes runtime dependencies.

2. **Version Selection**: The `istioVersion` should match one of the supported
   versions in the embedded resources. Use `istioversion.List()` to get available
   versions.

3. **Run-to-Completion**: The installer performs one-shot installation and returns.
   It does not run as a controller. The calling operator is responsible for
   reconciliation logic.
