package receiver

import "github.com/earthy1024/tallyd/adapter"

// Rule routes events matching EventName to Providers. An empty EventName
// never matches and is reserved for the router's Default.
type Rule struct {
	EventName string
	Providers []string
}

// StaticRouter implements Router with a fixed default provider list plus
// simple event-name match rules.
type StaticRouter struct {
	Default []string
	Rules   []Rule
}

func (r *StaticRouter) Route(e adapter.Event) []string {
	for _, rule := range r.Rules {
		if rule.EventName == e.EventName {
			return rule.Providers
		}
	}
	return r.Default
}
