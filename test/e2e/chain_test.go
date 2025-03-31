// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	e2einteractive "github.com/efficientgo/e2e/interactive"
	"github.com/efficientgo/e2e/monitoring/matchers"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/thanos-io/thanos/pkg/block"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	"github.com/thanos-io/objstore/providers/s3"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

//func TestBlockRead(t *testing.T) {
//	logger := log.NewLogfmtLogger(os.Stdout)
//
//	justAfterConsistencyDelay := 30 * time.Minute
//	// Make sure to take realistic timestamp for start. This is to align blocks as if they would be aligned on Prometheus.
//	// To have deterministic compaction, let's have fixed date:
//	now, err := time.Parse(time.RFC3339, "2020-03-24T08:00:00Z")
//	testutil.Ok(t, err)
//
//	e, err := e2e.New(e2e.WithName("chain-block-read"))
//	testutil.Ok(t, err)
//	t.Cleanup(e2ethanos.CleanScenario(t, e))
//
//	dir := filepath.Join(e.SharedDir(), "tmp")
//	testutil.Ok(t, os.MkdirAll(dir, os.ModePerm))
//
//	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
//	t.Cleanup(cancel)
//
//	receiverBlocks := []blockDesc{
//		{
//			series:   []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
//			extLset:  labels.FromStrings("case", "compaction-ready"),
//			mint:     timestamp.FromTime(now.Add(0 * time.Hour)),
//			maxt:     timestamp.FromTime(now.Add(2 * time.Hour)),
//			hashFunc: metadata.SHA256Func,
//		},
//	}
//
//	var receiverBlocksIds []ulid.ULID
//	for _, b := range receiverBlocks {
//		id, err := b.CreateWithValueGenerator(ctx, dir, justAfterConsistencyDelay, b.hashFunc, 120, nil, func(_ labels.Labels, index int) float64 {
//			if index%2 == 0 {
//				return 0.0
//			} else {
//				return 1.0
//			}
//		})
//		testutil.Ok(t, err)
//
//		receiverBlocksIds = append(receiverBlocksIds, id)
//	}
//
//	for _, id := range receiverBlocksIds {
//		series := blockSeries(t, logger, path.Join(dir, id.String()))
//		for _, s := range series {
//			fmt.Println(s.Labels)
//			for _, sample := range s.Samples {
//				fmt.Printf("Sample: %v\n", sample)
//			}
//		}
//	}
//}

func blockSeries(t *testing.T, logger log.Logger, blockPath string) []prompb.TimeSeries {
	b, err := tsdb.OpenBlock(nil, blockPath, nil, nil)
	testutil.Ok(t, err)
	defer runutil.CloseWithLogOnErr(logger, b, "block reader")

	chunkr, err := b.Chunks()
	testutil.Ok(t, err)
	defer runutil.CloseWithLogOnErr(logger, chunkr, "chunk reader")

	indexr, err := b.Index()
	testutil.Ok(t, err)

	postings, err := indexr.Postings(context.TODO(), "", "")
	testutil.Ok(t, err)
	defer runutil.CloseWithLogOnErr(logger, chunkr, "index reader")

	var series []prompb.TimeSeries
	for postings.Next() {
		var builder labels.ScratchBuilder
		var chMeta []chunks.Meta

		ref := postings.At()
		err := indexr.Series(ref, &builder, &chMeta)
		testutil.Ok(t, err)

		var samples []prompb.Sample
		for _, m := range chMeta {
			chk, chkIt, err := chunkr.ChunkOrIterable(m)
			testutil.Ok(t, err)
			testutil.Equals(t, chkIt, nil)
			it := chk.Iterator(nil)
			for it.Next() != chunkenc.ValNone {
				ts, v := it.At()
				samples = append(samples, prompb.Sample{
					Timestamp: ts,
					Value:     v,
				})
			}
		}

		series = append(series, prompb.TimeSeries{
			Labels:  prompb.FromLabels(builder.Labels(), nil),
			Samples: samples,
		})
	}

	return series
}

func TestChainDeduplicationWithStoreGateway(t *testing.T) {
	t.Parallel()
	logger := log.NewLogfmtLogger(os.Stdout)

	justAfterConsistencyDelay := 30 * time.Minute
	// Make sure to take realistic timestamp for start. This is to align blocks as if they would be aligned on Prometheus.
	// To have deterministic compaction, let's have fixed date:
	now, err := time.Parse(time.RFC3339, "2020-03-24T08:00:00Z")
	testutil.Ok(t, err)

	e, err := e2e.New(e2e.WithName("chain-dedup-gw"))
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	receiver1 := e2ethanos.NewReceiveBuilder(e, "1").WithIngestionEnabled().Init()
	receiver2 := e2ethanos.NewReceiveBuilder(e, "2").WithIngestionEnabled().Init()
	testutil.Ok(t, e2e.StartAndWaitReady(receiver1, receiver2))

	receiverBlocks := []blockDesc{
		{
			series: []labels.Labels{labels.FromStrings("a", "1", "b", "2", "case", "no-overlaps")},
			mint:   timestamp.FromTime(now.Add(0 * time.Hour)),
			maxt:   timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		{
			series: []labels.Labels{labels.FromStrings("a", "1", "b", "2", "case", "overlap-with-receiver")},
			mint:   timestamp.FromTime(now.Add(0 * time.Hour)),
			maxt:   timestamp.FromTime(now.Add(2 * time.Hour)),
		},
	}

	blocks := []blockDesc{
		{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("case", "compaction-ready", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
			//markedForNoCompact: true,
			hashFunc: metadata.SHA256Func,
		},
		{
			series:   []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset:  labels.FromStrings("case", "compaction-ready", "replica", "2"),
			mint:     timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:     timestamp.FromTime(now.Add(4 * time.Hour)),
			hashFunc: metadata.SHA256Func,
		},

		{
			series:   []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset:  labels.FromStrings("case", "downsample-ready", "replica", "1"),
			mint:     timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:     timestamp.FromTime(now.Add(42 * time.Hour)),
			hashFunc: metadata.SHA256Func,
		},
		{
			series:   []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset:  labels.FromStrings("case", "downsample-ready", "replica", "2"),
			mint:     timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:     timestamp.FromTime(now.Add(42 * time.Hour)),
			hashFunc: metadata.SHA256Func,
		},
	}

	dir := filepath.Join(e.SharedDir(), "tmp")
	testutil.Ok(t, os.MkdirAll(dir, os.ModePerm))

	const bucket = "compact-test"
	m := e2edb.NewMinio(e, "minio", bucket, e2edb.WithMinioTLS())
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	bkt, err := s3.NewBucketWithConfig(logger,
		e2ethanos.NewS3Config(bucket, m.Endpoint("http"), m.Dir()), "test-feed", nil)
	testutil.Ok(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	var receiverBlocksIds []ulid.ULID
	for _, b := range receiverBlocks {
		id, err := b.CreateWithValueGenerator(ctx, dir, justAfterConsistencyDelay, b.hashFunc, 120, nil, func(_ labels.Labels, index int) float64 {
			if index%2 == 0 {
				return 0.0
			} else {
				return 1.0
			}
		})
		testutil.Ok(t, err)

		receiverBlocksIds = append(receiverBlocksIds, id)
	}

	var receiverSeries []prompb.TimeSeries
	for _, id := range receiverBlocksIds {
		series := blockSeries(t, logger, path.Join(dir, id.String()))
		for _, s := range series {
			for _, sample := range s.Samples {
				receiverSeries = append(receiverSeries, prompb.TimeSeries{
					Labels:  s.Labels,
					Samples: []prompb.Sample{{Timestamp: sample.Timestamp, Value: sample.Value}},
				})
			}
		}
	}
	sort.Slice(receiverSeries, func(i, j int) bool {
		return receiverSeries[i].Samples[0].Timestamp < receiverSeries[j].Samples[0].Timestamp
	})

	for i, series := range receiverSeries {
		fmt.Printf("Sample: %d %v=%v %v\n", i, series.Samples[0].Timestamp, series.Samples[0].Value, series.Labels)

		if i == 103 {
			fmt.Println("failure point")
			//break
		}

		testutil.Ok(t, remoteWrite(context.Background(), []prompb.TimeSeries{series}, receiver1.Endpoint("remote-write")))
		testutil.Ok(t, remoteWrite(context.Background(), []prompb.TimeSeries{series}, receiver2.Endpoint("remote-write")))
		fmt.Println("written")
	}

	rawBlockIDs := map[ulid.ULID]struct{}{}
	for _, b := range blocks {
		id, err := b.CreateWithValueGenerator(ctx, dir, justAfterConsistencyDelay, b.hashFunc, 120, nil, func(_ labels.Labels, index int) float64 {
			if index%2 == 0 {
				return 0.0
			} else {
				return 1.0
			}
		})
		testutil.Ok(t, err)

		rawBlockIDs[id] = struct{}{}
		testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
			return objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String())
		}))

		if b.markedForNoCompact {
			testutil.Ok(t, block.MarkForNoCompact(ctx, logger, bkt, id, metadata.ManualNoCompactReason, "why not", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))
		}

		rawBlockIDs[id] = struct{}{}
	}

	bktConfig := client.BucketConfig{
		Type:   client.S3,
		Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("http"), m.InternalDir()),
	}

	// Crank down the deletion mark delay since deduplication can miss blocks in the presence of replica labels it doesn't know about.
	str := e2ethanos.NewStoreGW(e, "1", bktConfig, "", "", []string{"--ignore-deletion-marks-delay=2s"})
	testutil.Ok(t, e2e.StartAndWaitReady(str))
	testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(float64(len(rawBlockIDs))), "thanos_blocks_meta_synced"))
	testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_sync_failures_total"))
	testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_modified"))

	q := e2ethanos.NewQuerierBuilder(e, "1", receiver1.InternalEndpoint("grpc"), receiver2.InternalEndpoint("grpc"), str.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	// Check if query detects current series, even if overlapped.
	queryAndAssert(t, ctx, q.Endpoint("http"),
		func() string {
			return fmt.Sprintf(`count_over_time({a="1"}[48h] offset %ds)`, int64(time.Since(now.Add(42*time.Hour)).Seconds()))
		},
		time.Now,
		promclient.QueryOptions{
			Deduplicate: false, // This should be false, so that we can be sure deduplication was offline.
		},
		model.Vector{
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "receive": "receive-1", "tenant_id": "default-tenant"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "receive": "receive-2", "tenant_id": "default-tenant"}},

			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready", "replica": "2"}},

			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "downsample-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "downsample-ready", "replica": "2"}},
		},
	)
	// Store view:
	testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(float64(len(rawBlockIDs))), "thanos_blocks_meta_synced"))
	testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_sync_failures_total"))
	testutil.Ok(t, str.WaitSumMetrics(e2emon.Equals(0), "thanos_blocks_meta_modified"))

	{
		extArgs := []string{"--deduplication.replica-label=replica", "--delete-delay=0s"}
		c := e2ethanos.NewCompactorBuilder(e, "working-dedup").Init(bktConfig, nil, extArgs...)
		testutil.Ok(t, e2e.StartAndWaitReady(c))

		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Greater(0), []string{"thanos_compact_iterations_total"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(4), []string{"thanos_compact_blocks_cleaned_total"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_compact_block_cleanup_failures_total"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_compact_aborted_partial_uploads_deletion_attempts_total"}, e2emon.WaitMissingMetrics()))
		//testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_compact_group_compactions_total"}, e2emon.WaitMissingMetrics()))
		//testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_compact_group_vertical_compactions_total"}, e2emon.WaitMissingMetrics()))
		//testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_compact_group_compactions_failures_total"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(2), []string{"thanos_compact_group_compaction_runs_started_total"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(2), []string{"thanos_compact_group_compaction_runs_completed_total"}, e2emon.WaitMissingMetrics()))

		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_compact_downsample_total"}, e2emon.WaitMissingMetrics()))
		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_compact_downsample_failures_total"}, e2emon.WaitMissingMetrics()))

		testutil.Ok(t, str.WaitSumMetricsWithOptions(
			e2emon.Equals(3),
			[]string{"thanos_blocks_meta_synced"},
			e2emon.WaitMissingMetrics(),
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "state", "loaded")),
		))
		testutil.Ok(t, str.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_blocks_meta_sync_failures_total"}, e2emon.WaitMissingMetrics()))

		testutil.Ok(t, c.WaitSumMetricsWithOptions(e2emon.Equals(0), []string{"thanos_compact_halted"}, e2emon.WaitMissingMetrics()))
		// Make sure compactor does not modify anything else over time.
		testutil.Ok(t, c.Stop())
	}

	//		const path = "graph?g0.expr=sum(continuous_app_metric0)%20by%20(cluster%2C%20replica)&g0.tab=0&g0.stacked=0&g0.range_input=2w&g0.max_source_resolution=0s&g0.deduplicate=0&g0.partial_response=0&g0.store_matches=%5B%5D&g0.end_input=2021-07-27%2000%3A00%3A00"
	//	testutil.Ok(t, e2einteractive.OpenInBrowser(fmt.Sprintf("http://%s/%s", query1.Endpoint("http"), path)))
	//	testutil.Ok(t, e2einteractive.OpenInBrowser(fmt.Sprintf("http://%s/%s", prom2.Endpoint("http"), path)))
	//
	//	// Tracing endpoint.
	//	testutil.Ok(t, e2einteractive.OpenInBrowser("http://"+j.Endpoint("http-front")))
	//	// Profiling Endpoint.
	//	testutil.Ok(t, p.OpenUserInterfaceInBrowser())
	//	// Monitoring Endpoint.
	//	testutil.Ok(t, m.OpenUserInterfaceInBrowser())
	//	testutil.Ok(t, e2einteractive.RunUntilEndpointHit())

	fmt.Printf("receiver1 grpc endpoint: %s\n", receiver1.Endpoint("grpc"))
	fmt.Printf("receiver2 grpc endpoint: %s\n", receiver2.Endpoint("grpc"))
	fmt.Printf("store gateway grpc endpoint: %s\n", str.Endpoint("grpc"))
	fmt.Printf("store gateway http endpoint: %s\n", str.Endpoint("http"))

	query := url.QueryEscape(`{a="1"}`)
	endTime := url.QueryEscape(`2020-03-26 05:00:00`)
	contextPath := fmt.Sprintf(`graph?g0.expr=%s&g0.tab=0&g0.stacked=0&g0.range_input=2d&g0.max_source_resolution=0s&g0.deduplicate=1&g0.partial_response=1&g0.engine=prometheus&g0.analyze=0&g0.end_input=%s&g0.step_input=60`, query, endTime)
	testutil.Ok(t, e2einteractive.OpenInBrowser(fmt.Sprintf("http://%s/%s", q.Endpoint("http"), contextPath)))
	testutil.Ok(t, e2einteractive.RunUntilEndpointHit())

	//	http://localhost:9090/graph?g0.expr=%7Ba%3D%221%22%7D&g0.tab=0&g0.stacked=0&g0.range_input=4h&g0.max_source_resolution=0s&g0.deduplicate=1&g0.partial_response=1&g0.store_matches=%5B%5D&g0.engine=prometheus&g0.analyze=0&g0.tenant=&g0.end_input=2020-03-24%2011%3A00%3A00&g0.moment_input=2020-03-24%2011%3A00%3A00
	//http://127.0.0.1:32840/loaded
}
