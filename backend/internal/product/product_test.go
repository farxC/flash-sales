package product

import "testing"

func TestNewProduct_ValidatesInvariants(t *testing.T) {
	cases := []struct {
		name         string
		productName  string
		valueInCents int64
		stock        int
		wantErr      error
	}{
		{"empty name", "", 100, 1, ErrEmptyName},
		{"negative price", "Widget", -1, 1, ErrNegativePrice},
		{"negative stock", "Widget", 100, -1, ErrNegativeStock},
		{"valid", "Widget", 100, 1, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewProduct("id-1", tc.productName, "desc", tc.valueInCents, tc.stock)
			if err != tc.wantErr {
				t.Fatalf("got err %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestDecrementStock_InvalidQuantity(t *testing.T) {
	for _, qty := range []int{0, -1} {
		p, err := NewProduct("id-1", "Widget", "desc", 100, 10)
		if err != nil {
			t.Fatalf("unexpected error building product: %v", err)
		}

		if err := p.DecrementStock(qty); err != ErrInvalidQuantity {
			t.Fatalf("qty %d: got err %v, want %v", qty, err, ErrInvalidQuantity)
		}
		if p.Stock() != 10 {
			t.Fatalf("qty %d: stock changed to %d, want unchanged 10", qty, p.Stock())
		}
	}
}

func TestDecrementStock_InsufficientStock(t *testing.T) {
	p, err := NewProduct("id-1", "Widget", "desc", 100, 5)
	if err != nil {
		t.Fatalf("unexpected error building product: %v", err)
	}

	if err := p.DecrementStock(6); err != ErrInsufficientStock {
		t.Fatalf("got err %v, want %v", err, ErrInsufficientStock)
	}
	if p.Stock() != 5 {
		t.Fatalf("stock changed to %d, want unchanged 5", p.Stock())
	}
}

func TestDecrementStock_Success(t *testing.T) {
	p, err := NewProduct("id-1", "Widget", "desc", 100, 5)
	if err != nil {
		t.Fatalf("unexpected error building product: %v", err)
	}

	if err := p.DecrementStock(5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Stock() != 0 {
		t.Fatalf("stock = %d, want 0", p.Stock())
	}

	if err := p.DecrementStock(1); err != ErrInsufficientStock {
		t.Fatalf("got err %v, want %v", err, ErrInsufficientStock)
	}
}

func TestReleaseStock_InvalidQuantity(t *testing.T) {
	for _, qty := range []int{0, -1} {
		p, err := NewProduct("id-1", "Widget", "desc", 100, 10)
		if err != nil {
			t.Fatalf("unexpected error building product: %v", err)
		}

		if err := p.ReleaseStock(qty); err != ErrInvalidQuantity {
			t.Fatalf("qty %d: got err %v, want %v", qty, err, ErrInvalidQuantity)
		}
		if p.Stock() != 10 {
			t.Fatalf("qty %d: stock changed to %d, want unchanged 10", qty, p.Stock())
		}
	}
}

func TestReleaseStock_Success(t *testing.T) {
	p, err := NewProduct("id-1", "Widget", "desc", 100, 5)
	if err != nil {
		t.Fatalf("unexpected error building product: %v", err)
	}

	if err := p.DecrementStock(5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Stock() != 0 {
		t.Fatalf("stock = %d, want 0", p.Stock())
	}

	if err := p.ReleaseStock(5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Stock() != 5 {
		t.Fatalf("stock = %d, want 5", p.Stock())
	}
}
