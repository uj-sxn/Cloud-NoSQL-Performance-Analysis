import matplotlib.pyplot as plt
import numpy as np

# ==========================================
# 1. FINAL DATASET
# ==========================================
# [Insert_Single, Read_Specific, Update_Specific, Delete_Specific]

mongo_data = [652.97, 548.91, 478.78, 448.08]
dynamo_data = [291.91, 430.48, 378.91, 38.37]
astra_data = [470.13, 382.06, 416.27, 47.58]

# ==========================================
# 2. GENERATE CHARTS
# ==========================================
labels = ['Insert (Single)', 'Read (Specific)', 'Update (Specific)', 'Delete (Specific)']
x = np.arange(len(labels))
width = 0.25

fig, ax = plt.subplots(figsize=(12, 7))

# Create Bars
rects1 = ax.bar(x - width, mongo_data, width, label='MongoDB', color='#13aa52') # Mongo Green
rects2 = ax.bar(x, dynamo_data, width, label='DynamoDB', color='#232f3e') # AWS Blue
rects3 = ax.bar(x + width, astra_data, width, label='Astra DB', color='#4f1f8b') # Cassandra Purple

# Styling
ax.set_ylabel('Latency (Milliseconds) - Lower is Better')
ax.set_title('Cloud Database Performance: CRUD Operations')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()
ax.grid(axis='y', linestyle='--', alpha=0.3)

# Add Labels on top of bars
def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax.annotate(f'{height:.0f}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=10, fontweight='bold')

autolabel(rects1)
autolabel(rects2)
autolabel(rects3)

plt.tight_layout()
plt.savefig('final_db_comparison.png')
plt.show()