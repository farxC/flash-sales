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
