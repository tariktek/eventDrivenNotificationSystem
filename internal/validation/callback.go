package validation

import (
	"net/netip"
	"net/url"
	"strings"
)

const callbackURLMaxLen = 2048

// checkCallbackURL performs the static half of SSRF protection: it rejects
// URLs that obviously target internal infrastructure. Host names that
// resolve to private addresses are caught again at connection time by the
// callback dispatcher, since DNS can change between validation and delivery.
func checkCallbackURL(c collector, raw string, opts Options) {
	if len(raw) > callbackURLMaxLen {
		c.add("callback_url", "must be at most %d characters", callbackURLMaxLen)
		return
	}
	u, err := url.Parse(raw)
	if err != nil || !u.IsAbs() || u.Host == "" {
		c.add("callback_url", "must be an absolute URL")
		return
	}

	switch u.Scheme {
	case "https":
	case "http":
		if !opts.AllowInsecureCallbacks {
			c.add("callback_url", "must use https")
			return
		}
	default:
		c.add("callback_url", "must use https")
		return
	}

	if u.User != nil {
		c.add("callback_url", "must not contain credentials")
		return
	}

	if opts.AllowPrivateCallbacks {
		return
	}
	// A trailing dot makes a fully qualified name ("localhost.") that
	// resolves the same way, so strip it before comparing.
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".internal") {
		c.add("callback_url", "must not point to an internal host")
		return
	}
	if ip, err := netip.ParseAddr(host); err == nil && !IsPublicIP(ip) {
		c.add("callback_url", "must not point to a private, loopback or link-local address")
	}
}

// IsPublicIP reports whether ip is routable on the public internet. It is
// exported for the callback dispatcher's connection-time check.
func IsPublicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() {
		// IsGlobalUnicast is false for loopback, link-local, multicast and
		// unspecified addresses.
		return false
	}
	for _, p := range nonPublicPrefixes {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}

// nonPublicPrefixes are special-purpose ranges (IANA registries) that the
// netip predicates above do not cover.
var nonPublicPrefixes = func() []netip.Prefix {
	cidrs := []string{
		"0.0.0.0/8",       // "this network"
		"100.64.0.0/10",   // carrier-grade NAT
		"192.0.0.0/24",    // IETF protocol assignments
		"192.0.2.0/24",    // documentation (TEST-NET-1)
		"198.18.0.0/15",   // benchmarking
		"198.51.100.0/24", // documentation (TEST-NET-2)
		"203.0.113.0/24",  // documentation (TEST-NET-3)
		"240.0.0.0/4",     // reserved, incl. 255.255.255.255
		"64:ff9b::/96",    // NAT64: embeds an IPv4 address, possibly private
		"64:ff9b:1::/48",  // local-use NAT64
		"100::/64",        // discard-only
		"2001::/32",       // Teredo: embeds an IPv4 address
		"2001:db8::/32",   // documentation
		"2002::/16",       // 6to4: embeds an IPv4 address, possibly private
	}
	out := make([]netip.Prefix, len(cidrs))
	for i, c := range cidrs {
		out[i] = netip.MustParsePrefix(c)
	}
	return out
}()
