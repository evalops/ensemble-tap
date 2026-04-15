package normalize

import "testing"

func TestBuildSubjectAndTypeSanitizeValues(t *testing.T) {
	subject := BuildSubject("siphon", "Stripe", "invoice-item", "Paid")
	if subject != "siphon.stripe.invoice_item.paid" {
		t.Fatalf("unexpected subject %q", subject)
	}

	eventType := BuildType("GitHub", "pull.request", "Merged")
	if eventType != "siphon.github.pull_request.merged" {
		t.Fatalf("unexpected type %q", eventType)
	}

	tenantScoped := BuildSubjectWithTenant("siphon", "tenant-42", "stripe", "invoice", "paid", true)
	if tenantScoped != "siphon.tenant_42.stripe.invoice.paid" {
		t.Fatalf("unexpected tenant subject %q", tenantScoped)
	}
}
