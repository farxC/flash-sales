package order

import "testing"

func mustItem(t *testing.T, productID string, quantity int, valueInCents int64) Item {
	t.Helper()
	it, err := NewItem(productID, quantity, valueInCents)
	if err != nil {
		t.Fatalf("unexpected error building item: %v", err)
	}
	return it
}

func TestNewItem_ValidatesInvariants(t *testing.T) {
	cases := []struct {
		name         string
		productID    string
		quantity     int
		valueInCents int64
		wantErr      error
	}{
		{"empty product id", "", 1, 100, ErrEmptyProductID},
		{"zero quantity", "p-1", 0, 100, ErrInvalidQuantity},
		{"negative quantity", "p-1", -1, 100, ErrInvalidQuantity},
		{"negative value", "p-1", 1, -1, ErrNegativeItemValue},
		{"valid", "p-1", 1, 100, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewItem(tc.productID, tc.quantity, tc.valueInCents)
			if err != tc.wantErr {
				t.Fatalf("got err %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestNewOrder_ValidatesInvariants(t *testing.T) {
	item := mustItem(t, "p-1", 1, 100)

	cases := []struct {
		name       string
		requestID  string
		consumerID string
		items      []Item
		wantErr    error
	}{
		{"empty request id", "", GuestConsumerID, []Item{item}, ErrEmptyRequestID},
		{"empty consumer id", "req-1", "", []Item{item}, ErrEmptyConsumerID},
		{"no items", "req-1", GuestConsumerID, nil, ErrNoItems},
		{"valid", "req-1", GuestConsumerID, []Item{item}, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewOrder(tc.requestID, tc.consumerID, tc.items)
			if err != tc.wantErr {
				t.Fatalf("got err %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestNewOrder_StartsPending(t *testing.T) {
	o, err := NewOrder("req-1", GuestConsumerID, []Item{mustItem(t, "p-1", 1, 100)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if o.Status() != StatusPending {
		t.Fatalf("status = %q, want %q", o.Status(), StatusPending)
	}
}

func TestNewOrder_ComputesTotalValue(t *testing.T) {
	items := []Item{
		mustItem(t, "p-1", 2, 500),
		mustItem(t, "p-2", 3, 100),
	}

	o, err := NewOrder("req-1", GuestConsumerID, items)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 2*500 + 3*100 = 1300
	if got, want := o.TotalValueInCents(), int64(1300); got != want {
		t.Fatalf("total = %d, want %d", got, want)
	}
}

func TestRehydrateOrder_SetsIDAndStatus(t *testing.T) {
	items := []Item{mustItem(t, "p-1", 2, 500)}

	o, err := RehydrateOrder("order-1", "req-1", GuestConsumerID, items, StatusApproved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if o.ID() != "order-1" {
		t.Fatalf("id = %q, want %q", o.ID(), "order-1")
	}
	if o.Status() != StatusApproved {
		t.Fatalf("status = %q, want %q", o.Status(), StatusApproved)
	}
	if got, want := o.TotalValueInCents(), int64(1000); got != want {
		t.Fatalf("total = %d, want %d", got, want)
	}
}

func TestOrder_MarkReserved(t *testing.T) {
	cases := []struct {
		from    Status
		wantErr error
	}{
		{StatusPending, nil},
		{StatusReserved, ErrInvalidTransition},
		{StatusApproved, ErrInvalidTransition},
		{StatusRejected, ErrInvalidTransition},
	}

	for _, tc := range cases {
		t.Run(string(tc.from), func(t *testing.T) {
			o, err := RehydrateOrder("order-1", "req-1", GuestConsumerID, []Item{mustItem(t, "p-1", 1, 100)}, tc.from)
			if err != nil {
				t.Fatalf("unexpected error building order: %v", err)
			}

			if err := o.MarkReserved(); err != tc.wantErr {
				t.Fatalf("got err %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil {
				if o.Status() != StatusReserved {
					t.Fatalf("status = %q, want %q", o.Status(), StatusReserved)
				}
			} else if o.Status() != tc.from {
				t.Fatalf("status changed to %q, want unchanged %q", o.Status(), tc.from)
			}
		})
	}
}

func TestOrder_MarkApproved(t *testing.T) {
	cases := []struct {
		from    Status
		wantErr error
	}{
		{StatusPending, ErrInvalidTransition},
		{StatusReserved, nil},
		{StatusApproved, ErrInvalidTransition},
		{StatusRejected, ErrInvalidTransition},
	}

	for _, tc := range cases {
		t.Run(string(tc.from), func(t *testing.T) {
			o, err := RehydrateOrder("order-1", "req-1", GuestConsumerID, []Item{mustItem(t, "p-1", 1, 100)}, tc.from)
			if err != nil {
				t.Fatalf("unexpected error building order: %v", err)
			}

			if err := o.MarkApproved(); err != tc.wantErr {
				t.Fatalf("got err %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil {
				if o.Status() != StatusApproved {
					t.Fatalf("status = %q, want %q", o.Status(), StatusApproved)
				}
			} else if o.Status() != tc.from {
				t.Fatalf("status changed to %q, want unchanged %q", o.Status(), tc.from)
			}
		})
	}
}

func TestOrder_MarkRejected(t *testing.T) {
	cases := []struct {
		from    Status
		wantErr error
	}{
		{StatusPending, nil},
		{StatusReserved, nil},
		{StatusApproved, ErrInvalidTransition},
		{StatusRejected, ErrInvalidTransition},
	}

	for _, tc := range cases {
		t.Run(string(tc.from), func(t *testing.T) {
			o, err := RehydrateOrder("order-1", "req-1", GuestConsumerID, []Item{mustItem(t, "p-1", 1, 100)}, tc.from)
			if err != nil {
				t.Fatalf("unexpected error building order: %v", err)
			}

			if err := o.MarkRejected("out of stock"); err != tc.wantErr {
				t.Fatalf("got err %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil {
				if o.Status() != StatusRejected {
					t.Fatalf("status = %q, want %q", o.Status(), StatusRejected)
				}
			} else if o.Status() != tc.from {
				t.Fatalf("status changed to %q, want unchanged %q", o.Status(), tc.from)
			}
		})
	}
}

// TestOrder_TerminalStates_RefuseAnyFurtherTransition stands in for
// the redelivery-safety guarantee this state machine buys the future
// idempotency work (roadmap item 9): a redelivered event that tries
// to re-apply an already-applied transition must be refused by the
// aggregate itself, regardless of which Mark* method is called.
func TestOrder_TerminalStates_RefuseAnyFurtherTransition(t *testing.T) {
	for _, terminal := range []Status{StatusApproved, StatusRejected} {
		t.Run(string(terminal), func(t *testing.T) {
			newOrder := func(t *testing.T) *Order {
				t.Helper()
				o, err := RehydrateOrder("order-1", "req-1", GuestConsumerID, []Item{mustItem(t, "p-1", 1, 100)}, terminal)
				if err != nil {
					t.Fatalf("unexpected error building order: %v", err)
				}
				return o
			}

			if err := newOrder(t).MarkReserved(); err != ErrInvalidTransition {
				t.Errorf("MarkReserved: got err %v, want %v", err, ErrInvalidTransition)
			}
			if err := newOrder(t).MarkApproved(); err != ErrInvalidTransition {
				t.Errorf("MarkApproved: got err %v, want %v", err, ErrInvalidTransition)
			}
			if err := newOrder(t).MarkRejected("reason"); err != ErrInvalidTransition {
				t.Errorf("MarkRejected: got err %v, want %v", err, ErrInvalidTransition)
			}
		})
	}
}
