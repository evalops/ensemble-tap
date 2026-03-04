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

	tenantScoped := BuildSubjectWithTenant("ensemble.tap", "tenant-42", "stripe", "invoice", "paid", true)
	if tenantScoped != "ensemble.tap.tenant_42.stripe.invoice.paid" {
		t.Fatalf("unexpected tenant subject %q", tenantScoped)
	}
}
