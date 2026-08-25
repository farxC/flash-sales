"use client";

import { useCallback, useEffect, useState } from "react";
import type { CheckoutResponse, OrderStatusEvent, Product } from "./types";

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8081";

const currencyFormatter = new Intl.NumberFormat("pt-BR", {
  style: "currency",
  currency: "BRL",
});

type BuyState =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "pending"; requestId: string }
  | { status: "approved"; requestId: string }
  | { status: "rejected"; requestId: string; reason?: string }
  | { status: "error"; message: string };

export default function Home() {
  const [products, setProducts] = useState<Product[]>([]);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [buyState, setBuyState] = useState<Record<string, BuyState>>({});

  const fetchProducts = useCallback(async () => {
    try {
      const res = await fetch(`${API_URL}/products`);
      if (!res.ok) {
        throw new Error(`failed to load products (status ${res.status})`);
      }
      const data: Product[] = await res.json();
      setProducts(data);
      setLoadError(null);
    } catch (err) {
      setLoadError(err instanceof Error ? err.message : "unknown error");
    }
  }, []);

  useEffect(() => {
    fetchProducts();
  }, [fetchProducts]);

  // Order outcomes arrive asynchronously over SSE, published by the
  // order-status broadcaster once the confirmation step (approve/reject)
  // has run -- this stream is broadcast to every connected browser, so
  // we ignore events that don't match a request we're actually waiting on.
  useEffect(() => {
    const source = new EventSource(`${API_URL}/events`);

    source.onmessage = (e) => {
      let event: OrderStatusEvent;
      try {
        event = JSON.parse(e.data);
      } catch {
        return;
      }

      setBuyState((prev) => {
        const current = prev[event.productId];
        if (
          !current ||
          current.status !== "pending" ||
          current.requestId !== event.requestId
        ) {
          return prev;
        }
        return {
          ...prev,
          [event.productId]:
            event.status === "approved"
              ? { status: "approved", requestId: event.requestId }
              : {
                  status: "rejected",
                  requestId: event.requestId,
                  reason: event.reason,
                },
        };
      });

      fetchProducts();
    };

    return () => source.close();
  }, [fetchProducts]);

  async function handleBuy(productId: string) {
    setBuyState((prev) => ({ ...prev, [productId]: { status: "loading" } }));

    try {
      const res = await fetch(`${API_URL}/checkout`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ productId, quantity: 1 }),
      });

      if (res.status === 202) {
        const data: CheckoutResponse = await res.json();
        setBuyState((prev) => ({
          ...prev,
          [productId]: { status: "pending", requestId: data.requestId },
        }));
        // Reflect the immediate reservation decrement; the final
        // approved/rejected outcome triggers another refetch when it
        // arrives over SSE.
        setTimeout(fetchProducts, 500);
        return;
      }

      const message = await res.text();
      setBuyState((prev) => ({
        ...prev,
        [productId]: { status: "error", message: message || `status ${res.status}` },
      }));
    } catch (err) {
      setBuyState((prev) => ({
        ...prev,
        [productId]: {
          status: "error",
          message: err instanceof Error ? err.message : "unknown error",
        },
      }));
    }
  }

  return (
    <div className="flex flex-col flex-1 items-center bg-zinc-50 font-sans dark:bg-black">
      <main className="flex w-full max-w-2xl flex-1 flex-col gap-8 px-6 py-16 sm:px-10">
        <header className="flex flex-col gap-2">
          <h1 className="text-2xl font-semibold tracking-tight text-black dark:text-zinc-50">
            flash-sales
          </h1>
          <p className="text-sm text-zinc-600 dark:text-zinc-400">
            Concurrent checkout demo — press buy and watch the stock count
            update as the backend processes the reservation.
          </p>
        </header>

        {loadError && (
          <p className="rounded-md bg-red-50 px-4 py-3 text-sm text-red-700 dark:bg-red-950 dark:text-red-300">
            Failed to load products: {loadError}
          </p>
        )}

        <ul className="flex flex-col gap-4">
          {products.map((product) => {
            const state = buyState[product.id] ?? { status: "idle" };
            const soldOut = product.stock <= 0;

            return (
              <li
                key={product.id}
                className="flex flex-col gap-3 rounded-lg border border-black/[.08] bg-white p-5 dark:border-white/[.145] dark:bg-zinc-950"
              >
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <h2 className="font-medium text-black dark:text-zinc-50">
                      {product.name}
                    </h2>
                    <p className="text-sm text-zinc-600 dark:text-zinc-400">
                      {product.description}
                    </p>
                  </div>
                  <span className="whitespace-nowrap font-medium text-black dark:text-zinc-50">
                    {currencyFormatter.format(product.valueInCents / 100)}
                  </span>
                </div>

                <div className="flex items-center justify-between gap-4">
                  <span className="text-sm text-zinc-600 dark:text-zinc-400">
                    {soldOut ? "Sold out" : `${product.stock} in stock`}
                  </span>
                  <button
                    onClick={() => handleBuy(product.id)}
                    disabled={
                      soldOut ||
                      state.status === "loading" ||
                      state.status === "pending"
                    }
                    className="rounded-full bg-foreground px-5 py-2 text-sm font-medium text-background transition-colors hover:bg-[#383838] disabled:cursor-not-allowed disabled:opacity-40 dark:hover:bg-[#ccc]"
                  >
                    {state.status === "loading"
                      ? "Buying..."
                      : state.status === "pending"
                        ? "Awaiting confirmation..."
                        : "Buy"}
                  </button>
                </div>

                {state.status === "pending" && (
                  <p className="text-xs text-zinc-500 dark:text-zinc-500">
                    Reserved — id {state.requestId}, waiting for order
                    confirmation
                  </p>
                )}
                {state.status === "approved" && (
                  <p className="text-xs text-green-600 dark:text-green-400">
                    Order approved — id {state.requestId}
                  </p>
                )}
                {state.status === "rejected" && (
                  <p className="text-xs text-red-600 dark:text-red-400">
                    Order rejected — id {state.requestId}
                    {state.reason ? `: ${state.reason}` : ""}
                  </p>
                )}
                {state.status === "error" && (
                  <p className="text-xs text-red-600 dark:text-red-400">
                    {state.message}
                  </p>
                )}
              </li>
            );
          })}
        </ul>
      </main>
    </div>
  );
}
