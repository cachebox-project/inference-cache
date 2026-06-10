package engineclient

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// PrefixCacheProbe is the by-construction canary: it sends the SAME token-ID
// prompt to an OpenAI-compatible engine twice and reports the change in vLLM's
// prefix-cache counters. A positive hit delta on the second (warm) request shows
// the engine cached exactly the token-ID prompt it was given — so a routing
// fingerprint computed over those same tokens is guaranteed to match the engine's
// cached prefix. It needs no router: it isolates the engine half of the
// guarantee (the tokenizer half is proven by pkg/tokenize, the fingerprint half
// by pkg/fingerprint).
type PrefixCacheProbe struct {
	Client     EngineClient // sends the token-ID prompt (typically NewOpenAI)
	HTTP       *http.Client // scrapes /metrics; defaults to http.DefaultClient
	EngineURL  string       // base URL, e.g. http://host:8000
	MetricsURL string       // optional; defaults to EngineURL + "/metrics"
	Model      string
}

// ProbeResult reports the warm-request prefix-cache deltas plus both completions.
type ProbeResult struct {
	HitsDelta    int // vllm:prefix_cache_hits_total change across the warm request
	QueriesDelta int // vllm:prefix_cache_queries_total change across the warm request
	Cold         Completion
	Warm         Completion
}

// Run fires the cold request (populating the cache), then measures the
// prefix-cache counters immediately before and after an identical warm request,
// so HitsDelta reflects only the warm request. A HitsDelta > 0 is the success
// signal.
func (p *PrefixCacheProbe) Run(ctx context.Context, tokens []uint32, params CompletionParams) (ProbeResult, error) {
	if p.Client == nil {
		return ProbeResult{}, errors.New("canary: PrefixCacheProbe.Client is nil")
	}
	metricsURL := p.MetricsURL
	if metricsURL == "" {
		metricsURL = strings.TrimRight(p.EngineURL, "/") + "/metrics"
	}
	httpClient := p.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	cold, err := p.Client.Complete(ctx, p.EngineURL, p.Model, tokens, params)
	if err != nil {
		return ProbeResult{}, fmt.Errorf("canary: cold request: %w", err)
	}

	hitsPre, err := scrapeCounter(ctx, httpClient, metricsURL, "vllm:prefix_cache_hits_total")
	if err != nil {
		return ProbeResult{}, fmt.Errorf("canary: scrape hits (pre): %w", err)
	}
	queriesPre, err := scrapeCounter(ctx, httpClient, metricsURL, "vllm:prefix_cache_queries_total")
	if err != nil {
		return ProbeResult{}, fmt.Errorf("canary: scrape queries (pre): %w", err)
	}

	warm, err := p.Client.Complete(ctx, p.EngineURL, p.Model, tokens, params)
	if err != nil {
		return ProbeResult{}, fmt.Errorf("canary: warm request: %w", err)
	}

	hitsPost, err := scrapeCounter(ctx, httpClient, metricsURL, "vllm:prefix_cache_hits_total")
	if err != nil {
		return ProbeResult{}, fmt.Errorf("canary: scrape hits (post): %w", err)
	}
	queriesPost, err := scrapeCounter(ctx, httpClient, metricsURL, "vllm:prefix_cache_queries_total")
	if err != nil {
		return ProbeResult{}, fmt.Errorf("canary: scrape queries (post): %w", err)
	}

	return ProbeResult{
		HitsDelta:    hitsPost - hitsPre,
		QueriesDelta: queriesPost - queriesPre,
		Cold:         cold,
		Warm:         warm,
	}, nil
}

// scrapeCounter sums every series of a Prometheus counter (ignoring labels) from
// a /metrics endpoint. vLLM reports prefix-cache counters as floats; we truncate
// to int since they are monotonic integer counts.
func scrapeCounter(ctx context.Context, client *http.Client, url, metric string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("metrics endpoint returned %s", resp.Status)
	}

	var sum float64
	sc := bufio.NewScanner(io.LimitReader(resp.Body, 8<<20))
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' || !strings.HasPrefix(line, metric) {
			continue
		}
		// The char after the name must be whitespace or '{', not another
		// identifier char — avoids prefix collisions (e.g. <metric>_bucket).
		rest := line[len(metric):]
		if rest == "" || (rest[0] != ' ' && rest[0] != '\t' && rest[0] != '{') {
			continue
		}
		// Skip an optional {labels} block (label values may contain spaces, so a
		// naive field split is wrong), then take the value token. In Prometheus
		// exposition the value is the FIRST token after the labels; an optional
		// timestamp follows it, so don't read the last token.
		if rest[0] == '{' {
			end := closingBrace(rest)
			if end < 0 {
				continue // unterminated label set
			}
			rest = rest[end+1:]
		}
		rest = strings.TrimSpace(rest)
		if sp := strings.IndexAny(rest, " \t"); sp >= 0 {
			rest = rest[:sp] // drop a trailing timestamp
		}
		if v, err := strconv.ParseFloat(rest, 64); err == nil {
			sum += v
		}
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	return int(sum), nil
}

// closingBrace returns the index of the '}' that closes the label set starting
// at s[0] == '{', skipping any '}' inside a double-quoted label value (honoring
// \" and \\ escapes). Returns -1 if the label set is unterminated.
func closingBrace(s string) int {
	inQuote, escaped := false, false
	for i := 1; i < len(s); i++ {
		switch c := s[i]; {
		case escaped:
			escaped = false
		case inQuote && c == '\\':
			escaped = true
		case c == '"':
			inQuote = !inQuote
		case c == '}' && !inQuote:
			return i
		}
	}
	return -1
}
