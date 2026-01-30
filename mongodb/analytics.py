"""MongoDB Analytics - Just 2 non-trivial aggregation pipelines"""
from pymongo import MongoClient, DESCENDING

class MongoDBAnalytics:
    def __init__(self):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client.ecommerce_project
    
    # ========== PIPELINE 1: PRODUCT POPULARITY ==========
    def product_popularity_analysis(self):
        """
        NON-TRIVIAL AGGREGATION PIPELINE 1
        Business Question: What are our top-performing products?
        """
        print("\n" + "="*60)
        print("PRODUCT POPULARITY ANALYSIS")
        print("="*60)
        
        pipeline = [
            # 1. Unwind items to analyze each product separately
            {"$unwind": "$items"},
            
            # 2. Group by product and calculate metrics
            {
                "$group": {
                    "_id": "$items.product_id",
                    "total_quantity": {"$sum": "$items.quantity"},
                    "total_revenue": {"$sum": {"$multiply": ["$items.quantity", "$items.unit_price"]}},
                    "order_count": {"$sum": 1},
                    "avg_price": {"$avg": "$items.unit_price"}
                }
            },
            
            # 3. Join with products collection
            {
                "$lookup": {
                    "from": "products",
                    "localField": "_id",
                    "foreignField": "product_id",
                    "as": "product_info"
                }
            },
            {"$unwind": "$product_info"},
            
            # 4. Filter active products
            {"$match": {"product_info.is_active": True}},
            
            # 5. Format results
            {
                "$project": {
                    "product_id": "$_id",
                    "product_name": "$product_info.name",
                    "category": "$product_info.category_id",
                    "quantity_sold": "$total_quantity",
                    "revenue": {"$round": ["$total_revenue", 2]},
                    "orders": "$order_count",
                    "avg_price": {"$round": ["$avg_price", 2]},
                    "current_stock": "$product_info.current_stock"
                }
            },
            
            # 6. Sort by revenue (highest first)
            {"$sort": {"revenue": DESCENDING}},
            
            # 7. Top 10 products
            {"$limit": 10}
        ]
        
        results = list(self.db.transactions.aggregate(pipeline))
        
        # Print results nicely
        print("\nTop 10 Products by Revenue:")
        print("-" * 100)
        for i, product in enumerate(results, 1):
            print(f"{i:2}. {product['product_name'][:30]:30} | "
                  f"Revenue: ${product['revenue']:8.2f} | "
                  f"Qty: {product['quantity_sold']:4} | "
                  f"Stock: {product['current_stock']}")
        
        return results
    
    # ========== PIPELINE 2: USER SEGMENTATION ==========
    def user_segmentation_analysis(self):
        """
        NON-TRIVIAL AGGREGATION PIPELINE 2
        Business Question: How can we segment our users by purchasing behavior?
        """
        print("\n" + "="*60)
        print("USER SEGMENTATION ANALYSIS")
        print("="*60)
        
        pipeline = [
            # 1. Group transactions by user
            {
                "$group": {
                    "_id": "$user_id",
                    "total_spent": {"$sum": "$total"},
                    "order_count": {"$sum": 1},
                    "last_purchase": {"$max": "$timestamp"}
                }
            },
            
            # 2. Calculate recency (days since last purchase) using server $$NOW
            {
                "$addFields": {
                    "recency_days": {
                        "$divide": [
                            {"$subtract": ["$$NOW", "$last_purchase"]},
                            1000 * 60 * 60 * 24  # Convert ms to days
                        ]
                    }
                }
            },
            
            # 3. Create segments
            {
                "$addFields": {
                    "value_segment": {
                        "$switch": {
                            "branches": [
                                {"case": {"$gte": ["$total_spent", 1000]}, "then": "VIP"},
                                {"case": {"$gte": ["$total_spent", 500]}, "then": "Gold"},
                                {"case": {"$gte": ["$total_spent", 100]}, "then": "Silver"},
                                {"case": {"$gt": ["$total_spent", 0]}, "then": "Bronze"}
                            ],
                            "default": "No purchases"
                        }
                    },
                    "frequency_segment": {
                        "$switch": {
                            "branches": [
                                {"case": {"$gte": ["$order_count", 10]}, "then": "Very Frequent"},
                                {"case": {"$gte": ["$order_count", 5]}, "then": "Frequent"},
                                {"case": {"$gte": ["$order_count", 2]}, "then": "Occasional"},
                                {"case": {"$eq": ["$order_count", 1]}, "then": "One-time"}
                            ],
                            "default": "No purchases"
                        }
                    }
                }
            },
            
            # 4. Group by segments
            {
                "$group": {
                    "_id": {
                        "value": "$value_segment",
                        "frequency": "$frequency_segment"
                    },
                    "user_count": {"$sum": 1},
                    "avg_spent": {"$avg": "$total_spent"},
                    "total_revenue": {"$sum": "$total_spent"}
                }
            },
            
            # 5. Format results
            {
                "$project": {
                    "value_segment": "$_id.value",
                    "frequency_segment": "$_id.frequency",
                    "user_count": 1,
                    "avg_spent": {"$round": ["$avg_spent", 2]},
                    "total_revenue": {"$round": ["$total_revenue", 2]}
                }
            },
            
            # 6. Sort by total revenue
            {"$sort": {"total_revenue": DESCENDING}}
        ]
        
        results = list(self.db.transactions.aggregate(pipeline))
        
        # Print results
        print("\nUser Segments by Value and Frequency:")
        print("-" * 80)
        for segment in results:
            print(f"{segment['value_segment']:8} | "
                  f"{segment['frequency_segment']:15} | "
                  f"Users: {segment['user_count']:4} | "
                  f"Avg spent: ${segment['avg_spent']:8.2f}")
        
        return results
    
    # ========== OPTIONAL: REVENUE ANALYTICS ==========
    def revenue_by_category(self):
        """Optional third pipeline for revenue analysis"""
        print("\n" + "="*60)
        print("REVENUE BY CATEGORY ANALYSIS")
        print("="*60)
        
        pipeline = [
            {"$unwind": "$items"},
            {
                "$lookup": {
                    "from": "products",
                    "localField": "items.product_id",
                    "foreignField": "product_id",
                    "as": "product_info"
                }
            },
            {"$unwind": "$product_info"},
            {
                "$group": {
                    "_id": "$product_info.category_id",
                    "total_revenue": {"$sum": {"$multiply": ["$items.quantity", "$items.unit_price"]}},
                    "total_quantity": {"$sum": "$items.quantity"},
                    "transaction_count": {"$sum": 1}
                }
            },
            {"$sort": {"total_revenue": DESCENDING}}
        ]
        
        results = list(self.db.transactions.aggregate(pipeline))
        
        print("\nRevenue by Category:")
        print("-" * 50)
        for category in results:
            print(f"Category {category['_id']:10} | "
                  f"Revenue: ${category['total_revenue']:10.2f} | "
                  f"Qty: {category['total_quantity']:6}")
        
        return results

# ========== SIMPLE RUNNER ==========
if __name__ == "__main__":
    analytics = MongoDBAnalytics()
    
    # Run just the 2 required pipelines
    print("Running MongoDB Analytics Pipelines...")
    print("-" * 60)
    
    # Pipeline 1: Product Popularity
    products = analytics.product_popularity_analysis()
    
    # Pipeline 2: User Segmentation
    users = analytics.user_segmentation_analysis()
    
    print("\n✅ Analysis Complete!")
    print(f"• Analyzed {len(products)} top products")
    print(f"• Segmented users into {len(users)} groups")