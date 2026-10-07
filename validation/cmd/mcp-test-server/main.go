// Command mcp-test-server runs the validation suite's MCP test server: four
// customer tools over MCP Streamable HTTP at /mcp, backed by a disposable
// SQLite database that records every call.
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/battujeevan/SentryGate-AI/validation/mcpserver"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8090", "listen address")
	db := flag.String("db", "mcp-test.db", "SQLite database path (created if missing)")
	targetArg := flag.String("target-argument", "customer_id", "tool argument that names the customer")
	lost := flag.String("lost-response-targets", "customer-lost-response", "comma-separated targets whose calls are applied and then lose their response")
	gated := flag.String("gated-targets", "customer-gated", "comma-separated targets whose calls wait for POST /gate/release")
	flag.Parse()

	srv, err := mcpserver.New(mcpserver.Config{
		DBPath:              *db,
		TargetArgument:      *targetArg,
		LostResponseTargets: split(*lost),
		GatedTargets:        split(*gated),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	log.Printf("mcp-test-server listening on http://%s/mcp (db=%s)", *addr, *db)
	hs := &http.Server{Addr: *addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(hs.ListenAndServe())
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
