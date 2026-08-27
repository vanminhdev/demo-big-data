// Buoi 5 - MongoDB - Demo tich hop RetailStream
// Chay bang: mongosh retailstream demo.mongodb.js
// Hoac dan tung khoi vao mongosh / MongoDB Compass Playground.

use("retailstream");

// ---------------------------------------------------------------
// V01. CRUD co ban tren products
// ---------------------------------------------------------------
// Create
db.products.insertOne({
  product_id: "PROD99999",
  category_id: "CAT01",
  category_name: "Dien tu",
  product_name: "San pham demo CRUD",
  brand: "DemoBrand",
  price: 199000,
  attributes: { color: "den", warranty_months: 12 },
  updated_at: new Date().toISOString(),
});

// Read
print("--- V01 Read ---");
printjson(db.products.findOne({ product_id: "PROD99999" }));

// Update
print("--- V01 Update ---");
printjson(
  db.products.updateOne(
    { product_id: "PROD99999" },
    { $set: { price: 179000 } }
  )
);

// Delete
print("--- V01 Delete ---");
printjson(db.products.deleteOne({ product_id: "PROD99999" }));

// ---------------------------------------------------------------
// V02. Tao index phuc vu truy van doanh thu theo thoi gian
// ---------------------------------------------------------------
print("--- V02 createIndex ---");
printjson(db.orders.createIndex({ order_time: 1, status: 1 }));

// ---------------------------------------------------------------
// V03. Aggregation Pipeline: doanh thu theo danh muc va thang
// (bien the referencing: orders + order_items + products)
// ---------------------------------------------------------------
print("--- V03 Aggregation: doanh thu theo danh muc/thang ---");
db.order_items.aggregate([
  {
    $lookup: {
      from: "orders",
      localField: "order_id",
      foreignField: "order_id",
      as: "order",
    },
  },
  { $unwind: "$order" },
  { $match: { "order.status": { $in: ["PAID", "SHIPPED"] } } },
  {
    $lookup: {
      from: "products",
      localField: "product_id",
      foreignField: "product_id",
      as: "product",
    },
  },
  { $unwind: "$product" },
  {
    $project: {
      category_name: "$product.category_name",
      month: { $substrCP: ["$order.order_time", 0, 7] }, // YYYY-MM
      line_total: { $multiply: ["$quantity", "$unit_price"] },
    },
  },
  {
    $group: {
      _id: { category: "$category_name", month: "$month" },
      revenue: { $sum: "$line_total" },
      order_item_count: { $sum: 1 },
    },
  },
  { $sort: { "_id.month": 1, revenue: -1 } },
]).forEach((doc) => printjson(doc));

// ---------------------------------------------------------------
// V04. explain() cho truy van dung index vua tao
// ---------------------------------------------------------------
print("--- V04 explain(executionStats) ---");
printjson(
  db.orders
    .find({ status: "PAID" })
    .sort({ order_time: 1 })
    .explain("executionStats")
);

// ---------------------------------------------------------------
// V05. So sanh embedding vs referencing (minh hoa cho sinh vien)
// ---------------------------------------------------------------
print("--- V05 embedding (orders_embedded) ---");
printjson(db.orders_embedded.findOne({ order_id: "ORD000001" }));

print("--- V05 referencing (order_items) ---");
db.order_items.find({ order_id: "ORD000001" }).forEach((doc) => printjson(doc));
