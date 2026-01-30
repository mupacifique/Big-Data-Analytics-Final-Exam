"""
Simple Visualization for MongoDB Analytics Results
Creates 3-4 meaningful visualizations for the project report
"""
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os
import sys

# Ensure local `mongodb` modules can be imported when running the script from project root
sys.path.insert(0, os.path.dirname(__file__))
from analytics import MongoDBAnalytics

# Determine repository root (parent of this file) and create outputs directory
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
OUTPUTS_DIR = os.path.join(REPO_ROOT, 'outputs')
os.makedirs(OUTPUTS_DIR, exist_ok=True)

def create_product_popularity_chart(product_results, save_path=None):
    """
    Visualization 1: Top Products by Revenue
    """
    if save_path is None:
        save_path = os.path.join(OUTPUTS_DIR, 'product_popularity.png')

    if not product_results:
        print("No product results to visualize")
        return
    
    df = pd.DataFrame(product_results)
    
    plt.figure(figsize=(14, 8))
    
    # Create horizontal bar chart
    bars = plt.barh(range(len(df)), df['revenue'], color='#2E86AB', alpha=0.8, height=0.7)
    
    # Customize chart
    plt.title('Top Products by Revenue', fontsize=18, fontweight='bold', pad=20)
    plt.xlabel('Revenue ($)', fontsize=12)
    plt.ylabel('Products', fontsize=12)
    
    # Set y-axis labels with product names (shortened)
    product_labels = [name[:25] + '...' if len(name) > 25 else name for name in df['product_name']]
    plt.yticks(range(len(df)), product_labels, fontsize=10)
    
    # Add revenue values on bars
    for i, bar in enumerate(bars):
        width = bar.get_width()
        plt.text(width * 1.01, bar.get_y() + bar.get_height()/2,
                f'${width:,.0f}', va='center', fontsize=10, fontweight='bold')
    
    # Add grid for readability
    plt.grid(axis='x', alpha=0.3, linestyle='--')
    
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    print(f"✅ Product popularity chart saved: {save_path}")
    plt.close()

def create_stock_analysis_chart(product_results, save_path=None):
    """
    Visualization 2: Stock vs Revenue Analysis
    Shows which products need restocking
    """
    if save_path is None:
        save_path = os.path.join(OUTPUTS_DIR, 'stock_analysis.png')

    if not product_results:
        print("No product results to visualize")
        return
    
    df = pd.DataFrame(product_results)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Plot 1: Stock levels
    colors = []
    for stock in df['current_stock']:
        if stock == 0:
            colors.append('#FF6B6B')  # Red for out of stock
        elif stock < 10:
            colors.append('#FFD166')  # Yellow for low stock
        else:
            colors.append('#06D6A0')  # Green for good stock
    
    bars1 = ax1.bar(range(len(df)), df['current_stock'], color=colors, alpha=0.8)
    ax1.set_title('Current Stock Levels of Top Products', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Products (Top 10 by Revenue)', fontsize=11)
    ax1.set_ylabel('Stock Level', fontsize=11)
    ax1.set_xticks(range(len(df)))
    ax1.set_xticklabels([f'Product {i+1}' for i in range(len(df))], rotation=45)
    ax1.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Add stock numbers on bars
    for bar in bars1:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}', ha='center', va='bottom', fontsize=9)
    
    # Plot 2: Stock vs Revenue Scatter
    scatter = ax2.scatter(df['current_stock'], df['revenue'], 
                         s=df['quantity_sold']*5, alpha=0.6, 
                         c=df['revenue'], cmap='viridis')
    
    ax2.set_title('Stock Levels vs Revenue (Bubble Size = Quantity Sold)', 
                 fontsize=14, fontweight='bold')
    ax2.set_xlabel('Current Stock', fontsize=11)
    ax2.set_ylabel('Total Revenue ($)', fontsize=11)
    ax2.grid(alpha=0.3, linestyle='--')
    
    # Add colorbar
    plt.colorbar(scatter, ax=ax2, label='Revenue ($)')
    
    # Highlight products needing restock
    low_stock = df[df['current_stock'] < 10]
    if not low_stock.empty:
        for idx, row in low_stock.iterrows():
            ax2.annotate(f"Restock!",
                        (row['current_stock'], row['revenue']),
                        textcoords="offset points",
                        xytext=(0,10),
                        ha='center',
                        fontsize=9,
                        color='red',
                        fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    print(f"✅ Stock analysis chart saved: {save_path}")
    plt.close()

def create_user_segmentation_chart(user_results, save_path=None):
    """
    Visualization 3: User Segmentation Analysis
    """
    if save_path is None:
        save_path = os.path.join(OUTPUTS_DIR, 'user_segmentation.png')

    if not user_results:
        print("No user results to visualize")
        return
    
    df = pd.DataFrame(user_results)
    
    plt.figure(figsize=(12, 8))
    
    # Create a matrix heatmap of user segments
    pivot_table = df.pivot_table(index='value_segment', 
                                 columns='frequency_segment', 
                                 values='total_revenue',
                                 aggfunc='sum',
                                 fill_value=0)
    
    # Create heatmap
    heatmap = plt.imshow(pivot_table.values, cmap='YlOrRd', aspect='auto')
    
    # Add labels
    plt.title('User Segmentation Revenue Heatmap', fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Frequency Segment', fontsize=12)
    plt.ylabel('Value Segment', fontsize=12)
    
    # Set tick labels
    plt.xticks(range(len(pivot_table.columns)), pivot_table.columns, rotation=45, ha='right')
    plt.yticks(range(len(pivot_table.index)), pivot_table.index)
    
    # Add text annotations
    for i in range(len(pivot_table.index)):
        for j in range(len(pivot_table.columns)):
            value = pivot_table.iloc[i, j]
            if value > 0:
                plt.text(j, i, f'${value/1000:,.0f}K', 
                        ha='center', va='center', 
                        color='white' if value > pivot_table.values.max()/2 else 'black',
                        fontweight='bold')
    
    # Add colorbar
    plt.colorbar(heatmap, label='Total Revenue ($)')
    
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    print(f"✅ User segmentation chart saved: {save_path}")
    plt.close()

def create_revenue_trend_chart(analytics, save_path=None):
    """
    Visualization 4: Revenue Trends Over Time
    """
    # Get daily revenue data
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$metadata.timestamp"}},
                "daily_revenue": {"$sum": "$financials.total"},
                "daily_orders": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}},
        {"$limit": 30}  # Last 30 days
    ]
    
    results = list(analytics.db.transactions.aggregate(pipeline))

    if save_path is None:
        save_path = os.path.join(OUTPUTS_DIR, 'revenue_trends.png')
    
    if not results:
        print("No revenue trend data to visualize")
        return
    
    df = pd.DataFrame(results)
    df['date'] = pd.to_datetime(df['_id'])
    df = df.sort_values('date')
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    # Plot 1: Revenue trend
    ax1.plot(df['date'], df['daily_revenue'], 
            marker='o', linewidth=2, markersize=6, color='#118AB2')
    ax1.fill_between(df['date'], df['daily_revenue'], alpha=0.3, color='#118AB2')
    
    ax1.set_title('Daily Revenue Trends (Last 30 Days)', fontsize=16, fontweight='bold', pad=20)
    ax1.set_xlabel('Date', fontsize=12)
    ax1.set_ylabel('Daily Revenue ($)', fontsize=12)
    ax1.grid(alpha=0.3, linestyle='--')
    
    # Format x-axis dates
    ax1.xaxis.set_tick_params(rotation=45)
    
    # Add value labels for peaks
    max_rev_idx = df['daily_revenue'].idxmax()
    ax1.annotate(f'Peak: ${df.loc[max_rev_idx, "daily_revenue"]:,.0f}',
                xy=(df.loc[max_rev_idx, 'date'], df.loc[max_rev_idx, 'daily_revenue']),
                xytext=(10, 10), textcoords='offset points',
                arrowprops=dict(arrowstyle='->', color='red'),
                fontsize=10, fontweight='bold')
    
    # Plot 2: Orders trend
    ax2.bar(df['date'], df['daily_orders'], 
           color='#06D6A0', alpha=0.7, width=0.8)
    
    ax2.set_title('Daily Orders (Last 30 Days)', fontsize=16, fontweight='bold', pad=20)
    ax2.set_xlabel('Date', fontsize=12)
    ax2.set_ylabel('Number of Orders', fontsize=12)
    ax2.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Format x-axis dates
    ax2.xaxis.set_tick_params(rotation=45)
    
    # Add average line
    avg_orders = df['daily_orders'].mean()
    ax2.axhline(y=avg_orders, color='red', linestyle='--', alpha=0.7, 
               label=f'Average: {avg_orders:.0f} orders/day')
    ax2.legend()
    
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    print(f"✅ Revenue trends chart saved: {save_path}")
    plt.close()

def create_summary_dashboard(analytics, save_path=None):
    """
    Bonus: Create a summary dashboard with key metrics
    """
    fig = plt.figure(figsize=(16, 10))
    
    # Get key metrics
    total_users = analytics.db.users.count_documents({})
    total_products = analytics.db.products.count_documents({})
    total_transactions = analytics.db.transactions.count_documents({})
    
    # Run revenue by category
    revenue_results = analytics.revenue_by_category()
    category_df = pd.DataFrame(revenue_results) if revenue_results else pd.DataFrame()
    
    # Layout using GridSpec
    gs = fig.add_gridspec(3, 3)
    
    # 1. Key Metrics (Top row)
    ax1 = fig.add_subplot(gs[0, :])
    ax1.axis('off')
    
    metrics_text = f"""
    📊 E-COMMERCE ANALYTICS DASHBOARD
    ============================================
    
    KEY METRICS:
    • Total Users: {total_users:,}
    • Total Products: {total_products:,}
    • Total Transactions: {total_transactions:,}
    
    """
    
    if not category_df.empty:
        top_category = category_df.iloc[0]
        metrics_text += f"""
    TOP PERFORMING CATEGORY:
    • Category: {top_category.get('category_name', 'N/A')}
    • Revenue: ${top_category.get('total_revenue', 0):,.2f}
    • Products Sold: {top_category.get('total_quantity_sold', 0):,}
        """
    
    ax1.text(0.1, 0.5, metrics_text, fontsize=12, 
            fontfamily='monospace', verticalalignment='center')
    
    # 2. Category Revenue (Middle left)
    if not category_df.empty and len(category_df) > 0:
        ax2 = fig.add_subplot(gs[1, 0])
        top_5_categories = category_df.head(5)
        colors = plt.cm.Set3(np.linspace(0, 1, len(top_5_categories)))
        wedges, texts, autotexts = ax2.pie(top_5_categories['total_revenue'], 
                                          labels=top_5_categories['category_name'],
                                          autopct='%1.1f%%',
                                          colors=colors,
                                          startangle=90)
        ax2.set_title('Revenue by Category (Top 5)', fontsize=11, fontweight='bold')
    
    # 3. Stock Status (Middle center)
    ax3 = fig.add_subplot(gs[1, 1])
    
    # Get stock statistics
    pipeline = [
        {"$match": {"is_active": True}},
        {"$group": {
            "_id": None,
            "out_of_stock": {"$sum": {"$cond": [{"$eq": ["$current_stock", 0]}, 1, 0]}},
            "low_stock": {"$sum": {"$cond": [{"$and": [
                {"$gt": ["$current_stock", 0]},
                {"$lte": ["$current_stock", 10]}
            ]}, 1, 0]}},
            "good_stock": {"$sum": {"$cond": [{"$gt": ["$current_stock", 10]}, 1, 0]}}
        }}
    ]
    
    stock_stats = list(analytics.db.products.aggregate(pipeline))
    
    if stock_stats:
        stats = stock_stats[0]
        labels = ['Out of Stock', 'Low Stock (<10)', 'Good Stock']
        sizes = [stats['out_of_stock'], stats['low_stock'], stats['good_stock']]
        colors = ['#FF6B6B', '#FFD166', '#06D6A0']
        
        wedges, texts, autotexts = ax3.pie(sizes, labels=labels, colors=colors,
                                          autopct='%1.1f%%', startangle=90)
        ax3.set_title('Product Stock Status', fontsize=11, fontweight='bold')
    
    # 4. Transaction Summary (Middle right)
    ax4 = fig.add_subplot(gs[1, 2])
    
    # Get payment method distribution
    payment_pipeline = [
        {"$group": {
            "_id": "$payment.method",
            "count": {"$sum": 1},
            "total": {"$sum": "$financials.total"}
        }},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    
    payment_data = list(analytics.db.transactions.aggregate(payment_pipeline))
    
    if payment_data:
        payment_df = pd.DataFrame(payment_data)
        bars = ax4.bar(payment_df['_id'], payment_df['count'], color='#073B4C', alpha=0.7)
        ax4.set_title('Payment Method Distribution', fontsize=11, fontweight='bold')
        ax4.set_xlabel('Payment Method')
        ax4.set_ylabel('Number of Transactions')
        ax4.tick_params(axis='x', rotation=45)
        
        # Add count labels
        for bar in bars:
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height):,}', ha='center', va='bottom', fontsize=9)
    
    # 5. Recommendations (Bottom row)
    ax5 = fig.add_subplot(gs[2, :])
    ax5.axis('off')
    
    recommendations = f"""
    🎯 BUSINESS RECOMMENDATIONS
    ============================================
    
    1. INVENTORY MANAGEMENT:
       • Monitor products with stock levels below 10
       • Consider automatic reorder points
       
    2. CUSTOMER SEGMENTS:
       • Focus retention efforts on VIP customers
       • Create targeted campaigns for each segment
       
    3. PRODUCT STRATEGY:
       • Promote top-performing products
       • Bundle complementary items
       
    4. REVENUE OPTIMIZATION:
       • Analyze peak sales periods
       • Optimize pricing based on demand
    """
    
    ax5.text(0.1, 0.5, recommendations, fontsize=10, 
            fontfamily='monospace', verticalalignment='center')
    
    plt.suptitle('E-commerce Analytics Dashboard', fontsize=18, fontweight='bold', y=0.98)
    plt.tight_layout()
    if save_path is None:
        save_path = os.path.join(OUTPUTS_DIR, 'summary_dashboard.png')
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    print(f"✅ Summary dashboard saved: {save_path}")
    plt.close()

def main():
    """
    Main function to run all visualizations
    """
    print("=" * 70)
    print("CREATING DATA VISUALIZATIONS FOR PROJECT REPORT")
    print("=" * 70)
    
    # Initialize analytics
    analytics = MongoDBAnalytics()
    
    # Get data for visualizations
    print("\n📊 Fetching data for visualizations...")
    product_results = analytics.product_popularity_analysis()
    user_results = analytics.user_segmentation_analysis()
    
    print("\n🖼️ Creating visualizations...")
    
    # Create all visualizations
    create_product_popularity_chart(product_results)
    create_stock_analysis_chart(product_results)
    create_user_segmentation_chart(user_results)
    create_revenue_trend_chart(analytics)
    
    # Optional: Create summary dashboard
    create_summary_dashboard(analytics)
    
    print("\n" + "=" * 70)
    print("VISUALIZATION SUMMARY")
    print("=" * 70)
    print("\n✅ Created 4 required visualizations:")
    print("   1. product_popularity.png - Top products by revenue")
    print("   2. stock_analysis.png - Stock vs revenue analysis")
    print("   3. user_segmentation.png - User segment heatmap")
    print("   4. revenue_trends.png - Daily revenue trends")
    print("\n✅ Bonus visualization created:")
    print("   • summary_dashboard.png - Comprehensive dashboard")
    print("\n📁 All files saved to 'outputs/' folder")
    print("=" * 70)

if __name__ == "__main__":
    main()
    