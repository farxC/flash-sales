export type Product = {
  id: string;
  name: string;
  description: string;
  valueInCents: number;
  stock: number;
};

export type CheckoutResponse = {
  requestId: string;
};

export type OrderStatusEvent = {
  requestId: string;
  productId: string;
  quantity: number;
  status: "approved" | "rejected";
  reason?: string;
};
