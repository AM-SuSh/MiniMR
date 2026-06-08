package udf

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"mapreduce/mr"
	"net/url"
	"regexp"
	"strings"
	"unicode"
)

var htmlTagRe = regexp.MustCompile(`<[^>]*>`)
var spaceRe = regexp.MustCompile(`\s+`)

type crawlRecord struct {
	URL       string `json:"url"`
	HTML      string `json:"html"`
	Timestamp string `json:"timestamp"`
}

func registerCrawlClean() {
	mr.RegisterMap("crawl_clean_map", CrawlCleanMap)
	mr.RegisterReduce("crawl_clean_reduce", CrawlCleanReduce)
}

// CrawlCleanMap parses JSONL crawl records and emits (domain, cleaned_text).
func CrawlCleanMap(filename string, contents string) []mr.KeyValue {
	var kvs []mr.KeyValue
	lines := strings.Split(contents, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var rec crawlRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}
		domain := extractDomain(rec.URL)
		if domain == "" {
			continue
		}
		cleaned := cleanHTML(rec.HTML)
		if cleaned == "" {
			continue
		}
		kvs = append(kvs, mr.KeyValue{Key: domain, Value: cleaned})
	}
	return kvs
}

// CrawlCleanReduce deduplicates and merges text per domain.
func CrawlCleanReduce(key string, values []string) string {
	seen := make(map[string]bool)
	var unique []string
	for _, v := range values {
		h := sha256.Sum256([]byte(v))
		hash := hex.EncodeToString(h[:])
		if seen[hash] {
			continue
		}
		seen[hash] = true
		unique = append(unique, v)
	}

	merged := strings.Join(unique, "\n---\n")
	result := map[string]interface{}{
		"count":        len(values),
		"unique_count": len(unique),
		"merged_text":  merged,
	}
	data, _ := json.Marshal(result)
	return string(data)
}

func extractDomain(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Host
}

func cleanHTML(html string) string {
	text := htmlTagRe.ReplaceAllString(html, " ")
	text = spaceRe.ReplaceAllString(text, " ")
	text = strings.TrimSpace(text)
	var b strings.Builder
	for _, r := range text {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			continue
		}
		if unicode.IsPrint(r) || r == '\n' || r == '\t' {
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}

// FormatCrawlResult is a helper for display.
func FormatCrawlResult(domain, jsonStr string) string {
	return fmt.Sprintf("%s: %s", domain, jsonStr)
}
