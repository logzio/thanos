// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	e2einteractive "github.com/efficientgo/e2e/interactive"
	"github.com/go-kit/log"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/prometheus/common/model"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestLimits(t *testing.T) {
	t.Parallel()

	t.Run("series_limit", func(t *testing.T) {
		/*
			The router_replication suite configures separate routing and ingesting components.
			It verifies that data ingested from Prometheus instances through the router is successfully replicated twice
			across the ingestors.

						   ┌────────────┐
						   │            │
						   │ Avalanche  │
						   │            │
						   └─────┬──────┘
								 │
							 ┌───▼────┐
							 │        │
							 │ Router │
				 ┌───────────│        │──────────┐
				 │           └────────┘          │
			┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
			│           │   │           │   │           │
			│ Ingestor1 │   │ Ingestor2 │   │ Ingestor3 │
			│           │   │           │   │           │
			└─────┬─────┘   └─────┬─────┘   └─────┬─────┘
				 │           ┌───▼───┐           │
				 │           │       │           │
				 └───────────► Query ◄───────────┘
							 │       │
							 └───────┘

			NB: Made with asciiflow.com - you can copy & paste the above there to modify.
		*/

		t.Parallel()
		e, err := e2e.NewDockerEnvironment("routerReplica")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, e))

		// Setup 3 ingestors.
		i1 := e2ethanos.NewReceiveBuilder(e, "i1").WithIngestionEnabled().
			WithExtArgs(map[string]string{
				"--store.limits.request-series": "500",
			}).Init()
		i2 := e2ethanos.NewReceiveBuilder(e, "i2").WithIngestionEnabled().
			WithExtArgs(map[string]string{
				"--store.limits.request-series": "500",
			}).Init()
		i3 := e2ethanos.NewReceiveBuilder(e, "i3").WithIngestionEnabled().
			WithExtArgs(map[string]string{
				"--store.limits.request-series": "500",
			}).Init()

		h := receive.HashringConfig{
			Endpoints: []receive.Endpoint{
				{Address: i1.InternalEndpoint("grpc")},
				{Address: i2.InternalEndpoint("grpc")},
				{Address: i3.InternalEndpoint("grpc")},
			},
		}

		// Setup 1 distributor with double replication
		r1 := e2ethanos.NewReceiveBuilder(e, "r1").WithRouting(3, h).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(i1, i2, i3, r1))

		avalanche1 := e2ethanos.NewAvalanche(e, "avalanche-1",
			e2ethanos.AvalancheOptions{
				MetricCount:    "1",
				SeriesCount:    "20",
				MetricInterval: "3600",
				SeriesInterval: "3600",
				ValueInterval:  "3600",

				ConstLabels: []string{"group=g0"},
				LabelCount:  "0",

				RemoteURL:           e2ethanos.RemoteWriteEndpoint(r1.InternalEndpoint("remote-write")),
				RemoteWriteInterval: "5s",
				RemoteBatchSize:     "5",
				RemoteRequestCount:  "5",

				TenantID: "tenant1",
			})

		testutil.Ok(t, e2e.StartAndWaitReady(avalanche1))

		q := e2ethanos.NewQuerierBuilder(e, "1", i1.InternalEndpoint("grpc"), i2.InternalEndpoint("grpc"), i3.InternalEndpoint("grpc")).
			WithReplicaLabels("receive").
			Init()
		testutil.Ok(t, e2e.StartAndWaitReady(q))

		qLimited := e2ethanos.NewQuerierBuilder(e, "2", i1.InternalEndpoint("grpc"), i2.InternalEndpoint("grpc"), i3.InternalEndpoint("grpc")).
			WithReplicaLabels("receive").
			WithExtArgs(map[string]string{
				"--store.limits.request-series": "500",
			}).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(qLimited))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(3), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, qLimited.WaitSumMetricsWithOptions(e2emon.Equals(3), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

		queryWaitAndAssert(t, ctx, q.Endpoint("http"), func() string { return `count({group="g0"})` }, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, model.Vector{
			&model.Sample{
				Metric: model.Metric{},
				Value:  model.SampleValue(20),
			},
		})

		avalanche2 := e2ethanos.NewAvalanche(e, "avalanche-2",
			e2ethanos.AvalancheOptions{
				MetricCount:    "1",
				SeriesCount:    "500",
				MetricInterval: "3600",
				SeriesInterval: "3600",
				ValueInterval:  "3600",

				ConstLabels: []string{"group=g1", "subgroup=sg1"},
				LabelCount:  "0",

				RemoteURL:           e2ethanos.RemoteWriteEndpoint(r1.InternalEndpoint("remote-write")),
				RemoteWriteInterval: "5s",
				RemoteBatchSize:     "5",
				RemoteRequestCount:  "5",

				TenantID: "tenant1",
			})

		testutil.Ok(t, e2e.StartAndWaitReady(avalanche2))

		queryWaitAndAssert(t, ctx, q.Endpoint("http"), func() string { return `count({group="g1", subgroup="sg1"})` }, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, model.Vector{
			&model.Sample{
				Metric: model.Metric{},
				Value:  model.SampleValue(500),
			},
		})

		avalanche3 := e2ethanos.NewAvalanche(e, "avalanche-3",
			e2ethanos.AvalancheOptions{
				MetricCount:    "1",
				SeriesCount:    "500",
				MetricInterval: "3600",
				SeriesInterval: "3600",
				ValueInterval:  "3600",

				ConstLabels: []string{"group=g1", "subgroup=sg2"},
				LabelCount:  "0",

				RemoteURL:           e2ethanos.RemoteWriteEndpoint(r1.InternalEndpoint("remote-write")),
				RemoteWriteInterval: "5s",
				RemoteBatchSize:     "5",
				RemoteRequestCount:  "5",

				TenantID: "tenant1",
			})

		testutil.Ok(t, e2e.StartAndWaitReady(avalanche3))

		queryWaitAndAssert(t, ctx, q.Endpoint("http"), func() string { return `count({group="g1", subgroup="sg2"})` }, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, model.Vector{
			&model.Sample{
				Metric: model.Metric{},
				Value:  model.SampleValue(500),
			},
		})

		now := time.Now()
		opts := promclient.QueryOptions{Deduplicate: true, PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT}
		testutil.Ok(t, runutil.RetryWithLog(log.NewLogfmtLogger(os.Stdout), 5*time.Second, ctx.Done(), func() error {
			if _, _, _, err := promclient.NewDefaultClient().QueryInstant(ctx, urlParse(t, "http://"+q.Endpoint("http")), `count({group="g1"})`, now, opts); err != nil {
				e := err.Error()
				if strings.Contains(e, "expanding series") && strings.Contains(e, "failed to send series: limit 500 violated") {
					return nil
				}
				return err
			}
			return fmt.Errorf("expected an error")
		}))

		// receive ingestors limit works per a selector, so the query still can touch more series
		queryAndAssert(t, ctx, q.Endpoint("http"), func() string { return `count({subgroup="sg1"}) + count({subgroup="sg2"})` }, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, model.Vector{
			&model.Sample{
				Metric: model.Metric{},
				Value:  model.SampleValue(1000),
			},
		})

		// query limit doesn't have an impact on number of touched series
		queryAndAssert(t, ctx, qLimited.Endpoint("http"), func() string { return `count({subgroup="sg1"}) + count({subgroup="sg2"})` }, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, model.Vector{
			&model.Sample{
				Metric: model.Metric{},
				Value:  model.SampleValue(1000),
			},
		})

		// neither on number of returned series
		queryAndAssert(t, ctx, qLimited.Endpoint("http"), func() string { return `count({subgroup="sg1"} or {subgroup="sg2"}) by (series_id, subgroup)` }, time.Now, promclient.QueryOptions{
			Deduplicate: true,
		}, model.Vector{
			&model.Sample{
				Metric: model.Metric{},
				Value:  model.SampleValue(1000),
			},
		})

		const path = "graph"
		testutil.Ok(t, e2einteractive.OpenInBrowser(fmt.Sprintf("http://%s/%s", q.Endpoint("http"), path)))

		testutil.Ok(t, e2einteractive.RunUntilEndpointHit())
	})

}
