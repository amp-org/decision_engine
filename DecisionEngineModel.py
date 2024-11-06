import pandas as pd

# Load the main CSV file and the flag CSV files
main_df = pd.read_csv('Customer.csv')  # Main file with customer details
member_flag_df = pd.read_csv('Member_flag.csv')  # Member flag CSV with member_id and member_flag
provider_flag_df = pd.read_csv('Provider_flag.csv')  # Provider flag CSV with provider_id and provider_flag
agent_flag_df = pd.read_csv('Agent_flag.csv')  # Agent flag CSV with agent_id and agent_flag
diagnosis_gender_df = pd.read_csv('Diagnosis_gender.csv')  # CSV with diagnosis and gender flag
Procedure_gender_df  = pd.read_csv('Proc_gender.csv')  # CSV with Procedure and gender flag
diagnosis_age_df = pd.read_csv('Diagnosis_age.csv')  # CSV with diagnosis and age range (min_age, max_age)
Procedure_age_df = pd.read_csv('Procedure_age.csv')  # CSV with procedure and age range (min_age, max_age)
Procedure_Procedure_df = pd.read_csv('Proc_Poc.csv')  # CSV with procedure procedure flag

# Step 1: Merge the flag data into the main CSV file

main_df['Member_watchlist_flag'] = main_df['Member_id'].apply(
    lambda x: member_flag_df[member_flag_df['Member_id'] == x]['Member_flag'].values[0] 
              if x in member_flag_df['Member_id'].values else 'No'
)

main_df['Provider_watchlist_flag'] = main_df['Provider_id'].apply(
    lambda x: provider_flag_df[provider_flag_df['Provider_id'] == x]['Provider_flag'].values[0] 
              if x in provider_flag_df['Provider_id'].values else 'No'
)

main_df['Agent_watchlist_flag'] = main_df['Agent_id'].apply(
    lambda x: agent_flag_df[agent_flag_df['Agent_id'] == x]['Agent_flag'].values[0] 
              if x in agent_flag_df['Agent_id'].values else 'No'
)

# Step 2: Add the new Diagnosis Gender flag
def get_diagnosis_gender_flag(row):
    # Check if the diagnosis exists in the Diagnosis_Gender file for the specific gender
    Gender = row['Gender']
    Diagnosis = row['Diagnosis']
    
    # Check if the combination of diagnosis and gender exists in the Diagnosis_Gender dataframe
    match = diagnosis_gender_df[(diagnosis_gender_df['Diagnosis'] == Diagnosis) & 
                                 (diagnosis_gender_df['Gender'] == Gender)]
    
    if not match.empty:
        return 'Yes' 
    else:
        return 'No'
main_df['Diagnosis_Gender_Flag'] = main_df.apply(get_diagnosis_gender_flag, axis=1)
# Step 2: Add the new Procedure Gender flag
def get_Procedure_gender_flag(row):
    # Check if the diagnosis exists in the Diagnosis_Gender file for the specific gender
    Gender = row['Gender']
    Procedure_1 = row['Procedure_1']
    
    # Check if the combination of diagnosis and gender exists in the Diagnosis_Gender dataframe
    match = Procedure_gender_df[(Procedure_gender_df['Procedure_1'] == Procedure_1) & 
                                 (Procedure_gender_df['Gender'] == Gender)]
    
    if not match.empty:
        return 'Yes' 
    else:
        return 'No'    


main_df['Procedure_Gender_Flag'] = main_df.apply(get_Procedure_gender_flag, axis=1)

# Add the Age-Diagnosis flag (based on age range)
def get_age_diagnosis_flag(row):
    Diagnosis = row['Diagnosis']
    age = row['Age']
    
    # Check if the diagnosis exists in the Diagnosis_Age dataframe
    match = diagnosis_age_df[diagnosis_age_df['Diagnosis'] == Diagnosis]
    
    if not match.empty:
        # Get the min_age and max_age for the diagnosis
        min_age = match['min_age'].values[0]
        max_age = match['max_age'].values[0]
        
        # Check if the age falls between min_age and max_age
        if min_age <= age <= max_age:
            return 'Yes'
        else:
            return 'No'
    else:
        return 'No'

# Add the new Age-Diagnosis flag column
main_df['Diagnosis_Age_Flag'] = main_df.apply(get_age_diagnosis_flag, axis=1)


# Add the Age-Procedure flag (based on age range)
def get_age_procedure_flag(row):
    Procedure_1 = row['Procedure_1']
    age = row['Age']
    
    # Check if the diagnosis exists in the Diagnosis_Age dataframe
    match = Procedure_age_df[Procedure_age_df['Procedure_1'] == Procedure_1]
    
    if not match.empty:
        # Get the min_age and max_age for the diagnosis
        min_age = match['min_age'].values[0]
        max_age = match['max_age'].values[0]
        
        # Check if the age falls between min_age and max_age
        if min_age <= age <= max_age:
            return 'Yes'
        else:
            return 'No'
    else:
        return 'No'

# Add the new Age-Procedure flag column
main_df['Procedure_Age_Flag'] = main_df.apply(get_age_procedure_flag, axis=1)


def get_Procedure_Procedure_flag(row):
    # Check if the diagnosis exists in the Diagnosis_Gender file for the specific gender
    Procedure_1 = row['Procedure_1']
    Procedure_2 = row['Procedure_2']
    
    # Check if the combination of diagnosis and gender exists in the Diagnosis_Gender dataframe
    match = Procedure_Procedure_df[(Procedure_Procedure_df['Procedure_1'] == Procedure_1) & 
                                 (Procedure_Procedure_df['Procedure_2'] == Procedure_2)]
    
    if not match.empty:
        return 'Yes' 
    else:
        return 'No'    
    
main_df['Procedure_Procedure_Flag'] = main_df.apply(get_Procedure_Procedure_flag, axis=1)

# Step 2: Save the updated DataFrame to a new CSV
main_df.to_csv('Decision_engine.csv', index=False)
print("Processing complete. Updated CSV saved as 'updated_main_with_names_watchlist.csv'.")
