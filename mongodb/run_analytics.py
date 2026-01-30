"""
Simple runner for MongoDB analytics
"""
from load_data import load_data
from analytics import MongoDBAnalytics

def main():
    print("Step 1: Loading data into MongoDB...")
    db = load_data()  # This loads the data
    
    print("\nStep 2: Running analytics pipelines...")
    analytics = MongoDBAnalytics()
    
    # Run the two required aggregation pipelines
    print("\n" + "="*60)
    print("REQUIRED AGGREGATION PIPELINES")
    print("="*60)
    
    # 1. Product Popularity Analysis
    print("\n📦 PIPELINE 1: Product Popularity Analysis")
    product_results = analytics.product_popularity_analysis()
    
    # 2. User Segmentation Analysis
    print("\n👥 PIPELINE 2: User Segmentation Analysis")
    user_results = analytics.user_segmentation_analysis()
    
    print("\n✅ All tasks completed successfully!")

if __name__ == "__main__":
    main()