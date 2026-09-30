import os
from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from kafka_producer import publish_order_event

app = FastAPI(title="Kafka Order Lab API")

# Intentionally simple for the lab: the API keeps order status in memory.
orders: dict[str, str] = {}


class CreateOrderRequest(BaseModel):
    orderId: str


class OrderEventRequest(BaseModel):
    status: Literal["PAID", "SHIPPED", "CANCELLED"]


class InternalStatusRequest(BaseModel):
    status: str


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/orders", status_code=202)
def create_order(body: CreateOrderRequest) -> dict:
    order_id = body.orderId
    if order_id in orders:
        raise HTTPException(status_code=409, detail="orderId already exists")

    # The request is accepted before the Kafka consumer finishes the work.
    orders[order_id] = "PROCESSING"
    event_id = publish_order_event(order_id, "CREATED")

    return {
        "orderId": order_id,
        "status": "PROCESSING",
        "eventId": event_id,
        "message": "Order accepted and event published to Kafka.",
    }


@app.post("/orders/{order_id}/events", status_code=202)
def add_order_event(order_id: str, body: OrderEventRequest) -> dict:
    if order_id not in orders:
        raise HTTPException(status_code=404, detail="orderId not found")

    orders[order_id] = "PROCESSING"
    event_id = publish_order_event(order_id, body.status)

    return {
        "orderId": order_id,
        "status": "PROCESSING",
        "eventId": event_id,
        "message": "Event accepted and published to Kafka.",
    }


@app.get("/orders/{order_id}")
def get_order(order_id: str) -> dict:
    status = orders.get(order_id)
    if status is None:
        raise HTTPException(status_code=404, detail="orderId not found")
    return {"orderId": order_id, "status": status}


@app.put("/internal/orders/{order_id}/status")
def update_order_status(order_id: str, body: InternalStatusRequest) -> dict:
    """Called by the consumer after a message is successfully processed."""
    if order_id not in orders:
        raise HTTPException(status_code=404, detail="orderId not found")

    orders[order_id] = body.status
    return {"orderId": order_id, "status": body.status}
