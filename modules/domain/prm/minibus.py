import pandas as pd

def passenger_level_flags(prm_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build per-row passenger-level vehicle flags based on Passenger ID and ArrDep

    Parameters
    ----------
    prm_df : pandas.DataFrame
        Must include: ['Passenger ID','A/D','Vehicle Type'].

    Returns
    -------
    pandas.DataFrame
        Same number of rows as prm_df, with three additional columns:
            - Used_Ambulift : int (0/1)
                Indicates whether the passenger (within the same A/D) ever used an Ambulift
            -Used_Minibus : int (0/1)
                Indicates whether the passenger (Within the same A/D) ever used a Mini Bus
            -PassengerType: str
                Classified as one of :
                    'Ambulift'
                    'Mini Bus'
                    'Both'
                    'No Vehicle'
    
    Flags reflect usage across all rows for each (Passenger ID, A/D group)
    """
    x = prm_df.copy()
    
    group_cols = ["Passenger ID", "A/D"]

    #per row flags usring transform
    x["Used_Ambulift"] = (
        x.groupby(group_cols)["Vehicle Type"]
         .transform(lambda s: int("Ambulift" in set(s)))
    )


    x["Used_Minibus"] = (
        x.groupby(group_cols)["Vehicle Type"]
         .transform(lambda s: int("Mini Bus" in set(s)))
    )

    #PassengerType classification
    x["PassengerType"] = "No Vehicle"
    x.loc[(x["Used_Ambulift"] == 1) & (x["Used_Minibus"] == 0), "PassengerType"] = "Ambulift Only"
    x.loc[(x["Used_Ambulift"] == 0) & (x["Used_Minibus"] == 1), "PassengerType"] = "Mini Bus Only"
    x.loc[(x["Used_Ambulift"] == 1) & (x["Used_Minibus"] == 1), "PassengerType"] = "Both"

    return x
