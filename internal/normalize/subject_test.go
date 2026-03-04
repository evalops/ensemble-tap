package normalize

import "testing"

func TestBuildSubjectAndTypeSanitizeValues(t *testing.T) {
	subject := BuildSubject("ensemble.tap", "Stripe", "invoice-item", "Paid")
	if subject != "ensemble.tap.stripe.invoice_item.paid" {
		t.Fatalf("unexpected subject %q", subject)
	}

	eventType := BuildType("GitHub", "pull.request", "Merged")
	if eventType != "ensemble.tap.github.pull_request.merged" {
		t.Fatalf("unexpected type %q", eventType)
	}
}
