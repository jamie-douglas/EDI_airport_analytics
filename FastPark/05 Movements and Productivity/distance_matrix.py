import pandas as pd
import numpy as np

# =====================================
# 1. INPUT DATA
# =====================================
locations = {
    "Returns": (55.9473509, -3.3635478),
    "Start_car": (55.9450377407229, -3.35687039232943),
    "Start_main": (55.94673952227775, -3.344471147484556),

    "Green": (55.94479697315591, -3.356078460436161),
    "Yellow": (55.945532953010236, -3.3556171205828815),
    "Red": (55.946380063515264, -3.3549733904728636),

    "HH": (55.94417927510782, -3.3539190406098554),
    "GG": (55.94483415809971, -3.3532860393028607),
    "FF": (55.94512254342808, -3.352695953338714),
    "EE": (55.94467194040878, -3.352073680867432),
    "AA": (55.94508649537948, -3.3510866279819496),
    "DD": (55.945278751250946, -3.3516123409318257),
    "BB": (55.94542294252819, -3.350292694139279),
    "CC": (55.9456692680515, -3.349595319818014),

    "Y": (55.94690835971778, -3.3450858172958458),
    "A": (55.946563523980714, -3.3439048949098766),
    "B": (55.946307937857036, -3.3430499940787204),
    "C": (55.94603206523564, -3.342140756290751),
    "D": (55.945739962660475, -3.341238763475087),
    "E": (55.94545191491191, -3.340296923570788),
    "F": (55.94516995061015, -3.339333348863218),
    "G": (55.944896098414915, -3.3384929378864263),
    "H": (55.944608044389334, -3.337485893674706),
    "J": (55.94446604513421, -3.3364679820890943),
    "K": (55.94419421654895, -3.3354971624589114),
    "M": (55.943936586197545, -3.33462414932345),
    "P": (55.94342888108913, -3.333629046874671),
    "Q": (55.94473050236615, -3.333503829754636),
    "S": (55.945569267114166, -3.334592417252973),
    "T": (55.946137600136446, -3.335762853377994),
    "U": (55.94682050537139, -3.3372524994283497),
}

# =====================================
# 2. FIXED DISTANCES (YOUR INPUT)
# =====================================
FIXED_DISTANCES = {
    ("Returns", "Start_car"): 800,
    ("Start_car", "Returns"): 800,
    ("Start_car", "Start_main"): 900,
    ("Start_main", "Start_car"): 900,
}

# =====================================
# 3. HAVERSINE FUNCTION
# =====================================
def haversine(coord1, coord2):
    R = 6371000
    lat1, lon1 = np.radians(coord1)
    lat2, lon2 = np.radians(coord2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))

    return R * c

# =====================================
# 4. BASE MATRIX (WITH FIX OVERRIDE)
# =====================================
names = list(locations.keys())
base_matrix = pd.DataFrame(index=names, columns=names)

for i in names:
    for j in names:

        if i == j:
            base_matrix.loc[i, j] = 0
        elif (i, j) in FIXED_DISTANCES:
            base_matrix.loc[i, j] = FIXED_DISTANCES[(i, j)]
        else:
            base_matrix.loc[i, j] = haversine(locations[i], locations[j])

base_matrix = base_matrix.astype(float)

# =====================================
# 5. ZONES
# =====================================
main = ["Y","A","B","C","D","E","F","G","H","J","K","M","P","Q","S","T","U"]

# =====================================
# 6. ROUTING LOGIC
# =====================================
def adjusted_distance(i, j, base):

    if i == j:
        return 0

    if i == "Returns":
        if j in main:
            return base.loc["Returns", "Start_main"] + base.loc["Start_main", j]
        else:
            return base.loc["Returns", "Start_car"] + base.loc["Start_car", j]

    if j == "Returns":
        if i in main:
            return base.loc[i, "Start_main"] + base.loc["Start_main", "Returns"]
        else:
            return base.loc[i, "Start_car"] + base.loc["Start_car", "Returns"]

    if (i in main and j not in main):
        return (
            base.loc[i, "Start_main"] +
            base.loc["Start_main", "Start_car"] +
            base.loc["Start_car", j]
        )

    if (j in main and i not in main):
        return (
            base.loc[i, "Start_car"] +
            base.loc["Start_car", "Start_main"] +
            base.loc["Start_main", j]
        )

    return base.loc[i, j]

# =====================================
# 7. FINAL MATRIX
# =====================================
adjusted_matrix = pd.DataFrame(index=names, columns=names)

for i in names:
    for j in names:
        adjusted_matrix.loc[i, j] = adjusted_distance(i, j, base_matrix)

adjusted_matrix = adjusted_matrix.astype(float).round(1)

# =====================================
# 8. EXPORT
# =====================================
locations_df = pd.DataFrame.from_dict(
    locations, orient="index", columns=["latitude", "longitude"]
).reset_index().rename(columns={"index": "location"})

output_file = "carpark_distance_matrix.xlsx"

with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    locations_df.to_excel(writer, sheet_name="locations", index=False)
    adjusted_matrix.to_excel(writer, sheet_name="distance_matrix")

print(f"✅ File saved: {output_file}")