import os
import pandas as pd
import matplotlib.pyplot as plt

# ========================================
# PATH CONFIGURATION
# ========================================

# Input: analytics outputs (Parquet files)
INPUT_PATH = (
    "C:/Users/pmuyiringire/OneDrive - Bank of Kigali/"
    "BIG DATA ANALYTICS/SEM 3/BIG DATA ANALYTICS/"
    "Assignments/Final Project/analytics/outputs"
)

# Output: visualization images
OUTPUT_PATH = (
    "C:/Users/pmuyiringire/OneDrive - Bank of Kigali/"
    "BIG DATA ANALYTICS/SEM 3/BIG DATA ANALYTICS/"
    "Assignments/Final Project/visualization/output"
)

# ========================================
# DEPENDENCY CHECK
# ========================================

try:
    import pyarrow
    print(f"✅ PyArrow version: {pyarrow.__version__}")
except ImportError:
    print("❌ ERROR: PyArrow not installed. Run: pip install pyarrow")
    exit(1)

# ========================================
# PATH VALIDATION
# ========================================

print("\n🔍 Checking paths...")
print(f"Input path: {INPUT_PATH}")
print(f"Output path: {OUTPUT_PATH}")

funnel_file = os.path.join(INPUT_PATH, "funnel_counts.parquet")
session_file = os.path.join(INPUT_PATH, "session_metrics.parquet")

print(f"Funnel file exists: {os.path.exists(funnel_file)}")
print(f"Session file exists: {os.path.exists(session_file)}")

if not os.path.exists(funnel_file) or not os.path.exists(session_file):
    print("\n❌ ERROR: Required Parquet files not found.")
    print("💡 Run analytics task first and verify analytics/outputs/")
    exit(1)

os.makedirs(OUTPUT_PATH, exist_ok=True)

# ========================================
# LOAD DATA
# ========================================

print("\n📊 Loading analytics data...")

funnel_df = pd.read_parquet(funnel_file)
session_df = pd.read_parquet(session_file)

print(f"✅ Funnel data loaded: {len(funnel_df)} rows")
print(f"✅ Session data loaded: {len(session_df)} rows")

# ========================================
# VISUALIZATIONS
# ========================================

print("\n🎨 Generating visualizations...\n")

# 1. Conversion Funnel (Bar Chart)
plt.figure(figsize=(10, 6))
plt.bar(
    funnel_df["event"],
    funnel_df["count"],
    color="steelblue",
    edgecolor="black"
)
plt.title("Conversion Funnel Stages", fontsize=16, fontweight="bold")
plt.xlabel("Event Stage")
plt.ylabel("Number of Events")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_PATH, "funnel.png"), dpi=300)
plt.close()
print("✅ 1/5 - Funnel bar chart saved")

# 2. Session Duration Distribution
plt.figure(figsize=(10, 6))
plt.hist(
    session_df["session_duration_seconds"],
    bins=30,
    color="coral",
    edgecolor="black",
    alpha=0.7
)
plt.title("Session Duration Distribution", fontsize=16, fontweight="bold")
plt.xlabel("Duration (seconds)")
plt.ylabel("Frequency")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_PATH, "session_duration.png"), dpi=300)
plt.close()
print("✅ 2/5 - Session duration histogram saved")

# 3. Events per User Distribution
plt.figure(figsize=(10, 6))
plt.hist(
    session_df["num_events"],
    bins=30,
    color="mediumseagreen",
    edgecolor="black",
    alpha=0.7
)
plt.title("Events per User Distribution", fontsize=16, fontweight="bold")
plt.xlabel("Number of Events")
plt.ylabel("Number of Users")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_PATH, "events_per_user.png"), dpi=300)
plt.close()
print("✅ 3/5 - Events per user histogram saved")

# 4. Session Duration Boxplot
plt.figure(figsize=(8, 6))
plt.boxplot(
    session_df["session_duration_seconds"],
    vert=True,
    patch_artist=True,
    boxprops=dict(facecolor="lightblue", color="black"),
    medianprops=dict(color="red", linewidth=2)
)
plt.title("Session Duration Spread", fontsize=16, fontweight="bold")
plt.ylabel("Duration (seconds)")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_PATH, "session_boxplot.png"), dpi=300)
plt.close()
print("✅ 4/5 - Session duration boxplot saved")

# 5. Professional Funnel Visualization
funnel_order = ["view_item", "add_to_cart", "begin_checkout", "purchase"]
funnel_sorted = (
    funnel_df.set_index("event")
    .loc[funnel_order]
    .reset_index()
)

counts = funnel_sorted["count"].values
stages = funnel_sorted["event"].values

conversion_rates = [
    100.0 if i == 0 else (counts[i] / counts[0]) * 100
    for i in range(len(counts))
]

plt.figure(figsize=(10, 8))
colors = plt.cm.Blues(range(len(stages), 0, -1))
y_pos = range(len(stages))

plt.barh(y_pos, counts, color=colors, edgecolor="black")

for i, (count, rate) in enumerate(zip(counts, conversion_rates)):
    plt.text(
        count + max(counts) * 0.02,
        i,
        f"{count:,} ({rate:.1f}%)",
        va="center",
        fontweight="bold"
    )

plt.yticks(y_pos, stages)
plt.xlabel("Number of Events")
plt.title("E-Commerce Conversion Funnel", fontsize=16, fontweight="bold")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_PATH, "funnel_professional.png"), dpi=300)
plt.close()
print("✅ 5/5 - Professional funnel chart saved")

# ========================================
# SUMMARY
# ========================================

print("\n" + "=" * 60)
print("📈 VISUALIZATION SUMMARY")
print("=" * 60)

print(f"\n📁 Output directory: {OUTPUT_PATH}")
print("\nGenerated files:")
print("  • funnel.png")
print("  • funnel_professional.png")
print("  • session_duration.png")
print("  • events_per_user.png")
print("  • session_boxplot.png")

print("\n📊 Key Metrics:")
print(f"  • Total funnel stages: {len(funnel_df)}")
print(f"  • Total users analyzed: {len(session_df)}")
print(f"  • Avg session duration: {session_df['session_duration_seconds'].mean():.2f} sec")
print(f"  • Avg events per user: {session_df['num_events'].mean():.2f}")

if "view_item" in funnel_df["event"].values and "purchase" in funnel_df["event"].values:
    view_count = funnel_df.loc[funnel_df["event"] == "view_item", "count"].values[0]
    purchase_count = funnel_df.loc[funnel_df["event"] == "purchase", "count"].values[0]
    conversion_rate = (purchase_count / view_count) * 100
    print(f"  • Overall conversion rate: {conversion_rate:.2f}%")

print("\n✅ Task 4 visualizations generated successfully!")
print("=" * 60)