def identify_and_remove_duplicates(df):

    if df.duplicated().sum() > 0:
       df_cleaned = df.drop_duplicates(keep= 'first')
       print("The shape of the data frame after removing duplicates is ", df_cleaned.shape)
    else:
        df_cleaned =df
        print("This data frame contain no duplicate")
    return df_cleaned



