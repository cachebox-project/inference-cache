// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"

type (
	KVCacheRuntimeAdapter   = adapterruntime.KVCacheRuntimeAdapter
	InitContainerProvider   = adapterruntime.InitContainerProvider
	Options                 = adapterruntime.Options
	Option                  = adapterruntime.Option
	RuntimeID               = adapterruntime.RuntimeID
	SupportedPair           = adapterruntime.SupportedPair
	SubscriberSidecarParams = adapterruntime.SubscriberSidecarParams
)

const (
	RuntimeVLLM                     = adapterruntime.RuntimeVLLM
	RuntimeSGLang                   = adapterruntime.RuntimeSGLang
	RuntimeReference                = adapterruntime.RuntimeReference
	LMCacheKernelCheckContainerName = adapterruntime.LMCacheKernelCheckContainerName
	AnnotationLMCacheKernelCheck    = adapterruntime.AnnotationLMCacheKernelCheck
	KernelCheckModeAuto             = adapterruntime.KernelCheckModeAuto
	KernelCheckModeReportOnly       = adapterruntime.KernelCheckModeReportOnly
	KernelCheckModeStrict           = adapterruntime.KernelCheckModeStrict
	KernelCheckModeOff              = adapterruntime.KernelCheckModeOff
	KernelCheckMsgOK                = adapterruntime.KernelCheckMsgOK
	KernelCheckMsgFailPrefix        = adapterruntime.KernelCheckMsgFailPrefix
	EnvKernelCheckStrict            = adapterruntime.EnvKernelCheckStrict
	DefaultSubscriberImage          = adapterruntime.DefaultSubscriberImage
	DefaultPolicyServerGRPCAddress  = adapterruntime.DefaultPolicyServerGRPCAddress
	SubscriberContainerName         = adapterruntime.SubscriberContainerName
)

var (
	RenderSubscriberSidecar     = adapterruntime.RenderSubscriberSidecar
	WithSubscriberImage         = adapterruntime.WithSubscriberImage
	WithPolicyServerGRPCAddress = adapterruntime.WithPolicyServerGRPCAddress
	NewRegistry                 = adapterruntime.NewRegistry
	NewReferenceAdapter         = adapterruntime.NewReferenceAdapter
)
