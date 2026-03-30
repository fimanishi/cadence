package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestGaugeMode(t *testing.T) {
	ts := tally.NewTestScope("", nil)
	findName := func(m MetricIdx) string {
		def, ok := MetricDefs[Common][m]
		if ok {
			return def.metricName.String()
		}
		def, ok = MetricDefs[History][m]
		if ok {
			return def.metricName.String()
		}
		t.Fatalf("MetricDef not found in common or history: %v", m)
		return "unknown"
	}

	orig := GaugeMigrationMetrics
	t.Cleanup(func() {
		GaugeMigrationMetrics = orig
	})

	GaugeMigrationMetrics = map[string]struct{}{
		findName(BaseCacheByteSize):           {},
		findName(BaseCacheByteSizeLimitGauge): {},
	}

	c := NewClient(ts, History, MigrationConfig{
		Gauge: GaugeMigration{
			// Default: ..., left at default value (timer)
			Names: map[string]bool{
				findName(BaseCacheByteSize):           true,  // timer mode
				findName(BaseCacheByteSizeLimitGauge): false, // gauge mode
			},
		},
	})
	scope := c.Scope(HistoryDescribeQueueScope) // scope doesn't matter for this test

	// migration metrics: call both old (timer) and new (gauge) paths
	scope.RecordTimer(BaseCacheByteSize, time.Second)
	scope.UpdateGauge(BaseCacheByteSize, 1.0)

	scope.RecordTimer(BaseCacheByteSizeLimitGauge, 2*time.Second)
	scope.UpdateGauge(BaseCacheByteSizeLimitGauge, 2.0)

	// not migrating -> always emit
	scope.RecordTimer(CadenceDcRedirectionClientLatency, 3*time.Second)
	scope.UpdateGauge(GlobalRatelimiterQuota, 3.0)

	s := ts.Snapshot()
	findMetric := func(idx MetricIdx) (timer, gauge bool) {
		name := findName(idx)
		for _, v := range s.Timers() {
			if v.Name() == name {
				t.Logf("found timer: %v = %v", v.Name(), v.Values())
				timer = true
				break
			}
		}
		for _, v := range s.Gauges() {
			if v.Name() == name {
				t.Logf("found gauge: %v = %v", v.Name(), v.Value())
				gauge = true
				break
			}
		}
		return
	}
	assertFound := func(idx MetricIdx, timer, gauge bool) {
		name := findName(idx)
		foundTimer, foundGauge := findMetric(idx)
		assert.Equalf(t, timer, foundTimer, "wrong timer behavior for %v", name)
		assert.Equalf(t, gauge, foundGauge, "wrong gauge behavior for %v", name)
	}

	// Names[...] = true: EmitTimer=true, EmitGauge=true → both paths emit
	assertFound(BaseCacheByteSize, true, true)
	// Names[...] = false: EmitTimer=false, EmitGauge=false → neither emits
	assertFound(BaseCacheByteSizeLimitGauge, false, false)
	// not migrating: always emit the type that was called
	assertFound(CadenceDcRedirectionClientLatency, true, false)
	assertFound(GlobalRatelimiterQuota, false, true)
}

func TestCounterMode(t *testing.T) {
	ts := tally.NewTestScope("", nil)
	findName := func(m MetricIdx) string {
		def, ok := MetricDefs[Common][m]
		if ok {
			return def.metricName.String()
		}
		def, ok = MetricDefs[History][m]
		if ok {
			return def.metricName.String()
		}
		t.Fatalf("MetricDef not found in common or history: %v", m)
		return "unknown"
	}

	orig := CounterMigrationMetrics
	t.Cleanup(func() {
		CounterMigrationMetrics = orig
	})

	CounterMigrationMetrics = map[string]struct{}{
		findName(CadenceRequests): {},
		findName(CadenceFailures): {},
	}

	c := NewClient(ts, History, MigrationConfig{
		Counter: CounterMigration{
			// Default: ..., left at default value (timer)
			Names: map[string]bool{
				findName(CadenceRequests): true,  // timer mode
				findName(CadenceFailures): false, // counter mode
			},
		},
	})
	scope := c.Scope(HistoryDescribeQueueScope) // scope doesn't matter for this test

	// migration metrics: call both old (timer) and new (counter) paths
	scope.RecordTimer(CadenceRequests, time.Second)
	scope.IncCounter(CadenceRequests)

	scope.RecordTimer(CadenceFailures, 2*time.Second)
	scope.IncCounter(CadenceFailures)

	// not migrating -> always emit
	scope.RecordTimer(CadenceDcRedirectionClientLatency, 3*time.Second)
	scope.IncCounter(CadenceErrBadRequestCounter)

	s := ts.Snapshot()
	findMetric := func(idx MetricIdx) (timer, counter bool) {
		name := findName(idx)
		for _, v := range s.Timers() {
			if v.Name() == name {
				t.Logf("found timer: %v = %v", v.Name(), v.Values())
				timer = true
				break
			}
		}
		for _, v := range s.Counters() {
			if v.Name() == name {
				t.Logf("found counter: %v = %v", v.Name(), v.Value())
				counter = true
				break
			}
		}
		return
	}
	assertFound := func(idx MetricIdx, timer, counter bool) {
		name := findName(idx)
		foundTimer, foundCounter := findMetric(idx)
		assert.Equalf(t, timer, foundTimer, "wrong timer behavior for %v", name)
		assert.Equalf(t, counter, foundCounter, "wrong counter behavior for %v", name)
	}

	// Names[...] = true: EmitTimer=true, EmitCounter=true → both paths emit
	assertFound(CadenceRequests, true, true)
	// Names[...] = false: EmitTimer=false, EmitCounter=false → neither emits
	assertFound(CadenceFailures, false, false)
	// not migrating: always emit the type that was called
	assertFound(CadenceDcRedirectionClientLatency, true, false)
	assertFound(CadenceErrBadRequestCounter, false, true)
}

func TestHistogramMode(t *testing.T) {
	ts := tally.NewTestScope("", nil)
	findName := func(m MetricIdx) string {
		def, ok := MetricDefs[Common][m]
		if ok {
			return def.metricName.String()
		}
		def, ok = MetricDefs[History][m]
		if ok {
			return def.metricName.String()
		}
		t.Fatalf("MetricDef not found in common or history: %v", m)
		return "unknown"
	}

	orig := HistogramMigrationMetrics
	t.Cleanup(func() {
		HistogramMigrationMetrics = orig
	})

	HistogramMigrationMetrics = map[string]struct{}{
		findName(CadenceLatency):                            {},
		findName(ExponentialReplicationTaskLatency):         {},
		findName(PersistenceLatencyPerShard):                {},
		findName(ExponentialTaskProcessingLatency):          {},
		findName(PersistenceLatency):                        {},
		findName(PersistenceLatencyHistogram):               {},
		findName(PersistenceLatencyHistogramPerHost):        {},
		findName(TaskAttemptTimer):                          {},
		findName(ExponentialTaskAttemptCounts):              {},
		findName(TaskQueueLatency):                          {},
		findName(ExponentialTaskQueueLatency):               {},
		findName(TaskLatencyPerDomain):                      {},
		findName(ExponentialTaskLatencyPerDomain):           {},
		findName(TaskAttemptTimerPerDomain):                 {},
		findName(ExponentialTaskAttemptCountsPerDomain):     {},
		findName(TaskProcessingLatencyPerDomain):            {},
		findName(ExponentialTaskProcessingLatencyPerDomain): {},
		findName(TaskQueueLatencyPerDomain):                 {},
		findName(ExponentialTaskQueueLatencyPerDomain):      {},
	}

	c := NewClient(ts, History, MigrationConfig{
		Histogram: HistogramMigration{
			// Default: ..., left at default value
			Names: map[string]bool{
				findName(CadenceLatency):                    true,  // timer type
				findName(ExponentialReplicationTaskLatency): false, // histogram type

				findName(PersistenceLatencyPerShard):       false, // timer type
				findName(ExponentialTaskProcessingLatency): true,  // histogram type
			},
		},
	})
	scope := c.Scope(HistoryDescribeQueueScope) // scope doesn't matter for this test

	scope.RecordTimer(CadenceLatency, time.Second)
	scope.ExponentialHistogram(ExponentialReplicationTaskLatency, 2*time.Second)

	scope.RecordTimer(PersistenceLatencyPerShard, 3*time.Second)
	scope.ExponentialHistogram(ExponentialTaskProcessingLatency, 4*time.Second)

	// unspecified -> default config
	scope.RecordTimer(PersistenceLatency, 5*time.Second)
	scope.RecordHistogramDuration(PersistenceLatencyHistogram, 6*time.Second)

	// not migrating -> always emit
	scope.RecordTimer(CadenceDcRedirectionClientLatency, 7*time.Second)
	scope.RecordHistogramDuration(GlobalRatelimiterStartupUsageHistogram, 8*time.Second)

	s := ts.Snapshot()
	findMetric := func(idx MetricIdx) (timer, histogram bool) {
		name := findName(idx)
		for _, v := range s.Timers() {
			if v.Name() == name {
				t.Logf("found timer: %v = %v", v.Name(), v.Values())
				timer = true
				break
			}
		}
		for _, v := range s.Histograms() {
			if v.Name() == findName(idx) {
				nzDur := make(map[time.Duration]int64, 1)
				for k, val := range v.Durations() {
					if val != 0 {
						nzDur[k] = val
					}
				}
				nzVal := make(map[float64]int64, 1)
				for k, val := range v.Values() {
					if val != 0 {
						nzVal[k] = val
					}
				}
				t.Logf("found histogram: %v = %v (values: %v)", v.Name(), nzDur, nzVal)
				histogram = true
				break
			}
		}
		return
	}
	assertFound := func(idx MetricIdx, timer, histogram bool) {
		name := findName(idx)
		foundTimer, foundHistogram := findMetric(idx)
		assert.Equalf(t, foundTimer, timer, "wrong timer behavior for %v", name)
		assert.Equalf(t, foundHistogram, histogram, "wrong histogram behavior for %v", name)
	}

	// only the timer
	assertFound(CadenceLatency, true, false)
	assertFound(ExponentialReplicationTaskLatency, false, false)
	// only the histogram
	assertFound(PersistenceLatencyPerShard, false, false)
	assertFound(ExponentialTaskProcessingLatency, false, true)
	// timers only (via default)
	assertFound(PersistenceLatency, true, false)
	assertFound(PersistenceLatencyHistogram, false, false)
	// not migrating, the correct type should be emitted
	assertFound(CadenceDcRedirectionClientLatency, true, false)
	assertFound(GlobalRatelimiterStartupUsageHistogram, false, true)

	// when fixing: check logs!  you should see metrics with values for: 1, 4, 7, 8
}
