import os
import requests
import schedule
import time
import uuid
import csv
from config import SOURCE_URL, TARGET_URL, VERSION_URL, ARCHIVE_PATH, DATA_PATH

def fetch_data(url):
    """Fetches data from the provided URL with pagination support and avoids duplicates."""
    all_data = []
    unique_study_ids = set() 
    
    while url:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                print("Data fetched successfully.")
                data = response.json()
                
                for study in data.get('studies', []):
                    # Extract the study ID (or any other unique identifier)
                    study_id = study.get('protocolSection', {}).get('identificationModule', {}).get('nctId', None)
                    
                    if study_id and study_id not in unique_study_ids:
                        unique_study_ids.add(study_id)
                        all_data.append(study)
                
                # Get the next page token from the response
                next_page_token = data.get('nextPageToken', None)
                if next_page_token:
                    
                    url = f"{SOURCE_URL}?pageToken={next_page_token}"
                else:
                   
                    url = None
            else:
                print(f"Failed to fetch data. Status Code: {response.status_code}")
                break
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break

    return {'studies': all_data}




def save_data(data, file_path, fieldnames):
    """Save data to a CSV file."""
    if data:
        # Ensure all dictionaries have the required fieldnames, adding default values if needed
        processed_data = []
        for item in data:
            # Create a dictionary with default values for missing fieldnames
            processed_item = {field: item.get(field, '') for field in fieldnames}
            processed_data.append(processed_item)
        
        # Write data to CSV
        with open(file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(processed_data)
    else:
        print(f"No data to save to {file_path}")


def upload_data(file_path, url):
    """Uploads data to a target URL (real file upload to Flask)."""
    print(f"Uploading data to {url}...")
    try:
        with open(file_path, 'rb') as file:
            files = {'file': file}
            response = requests.post(url, files=files)
            
        if response.status_code == 200:
            print("File uploaded successfully.")
        else:
            print(f"Failed to upload file. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error uploading file: {e}")
        


def move_to_archive(file_path):
    """Moves the file to the archive folder."""
    if not os.path.exists(ARCHIVE_PATH):
        os.makedirs(ARCHIVE_PATH)
    os.rename(file_path, os.path.join(ARCHIVE_PATH, os.path.basename(file_path)))
    print(f"Moved file to archive: {os.path.basename(file_path)}")

def get_existing_data(file_path):
    """Read existing data from a CSV file."""
    if not os.path.exists(file_path):
        return []
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return list(reader)
    
    
def process_data():
    """Main function to process data from both endpoints."""
    # Fetch and process studies data
    studies_data = fetch_data(SOURCE_URL)
    
    if studies_data:
        
        processed_studies = []
        processed_organizations = []
        processed_statuses = []
        processed_sponsors = []
        processed_collaborators = []
        processed_conditions = []
        processed_designs = []
        processed_arms = []
        processed_interventions = []
        processed_outcomes = []
        processed_eligibility = []
        processed_contacts = []
        processed_locations = []

        # Dictionary to map organization names to IDs
        organization_id_map = {}
        next_organization_id = 1

        for study in studies_data.get('studies', []):
            protocol_section = study.get('protocolSection', {})
            identification_module = protocol_section.get('identificationModule', {})
            status_module = protocol_section.get('statusModule', {})
            sponsor_collaborators_module = protocol_section.get('sponsorCollaboratorsModule', {})
            oversight_module = protocol_section.get('oversightModule', {})
            description_module = protocol_section.get('descriptionModule', {})
            design_module = study.get('designModule', {})
            arms_interventions_module = study.get('armsInterventionsModule', {})
            intervention = study.get('interventionsModule', {})
            outcomes_module = study.get('outcomesModule', {})
            eligibility_module = study.get('eligibilityModule', {})
            contacts_locations_module = study.get('contactsModule', {})
            locations_module = protocol_section.get('contactsLocationsModule', {}).get('locations', [])  

            study_id = identification_module.get('nctId', None)
            brief_title = identification_module.get('briefTitle', None)
            official_title = identification_module.get('officialTitle', None)
            acronym = identification_module.get('orgStudyIdInfo', {}).get('id', None)
            start_date = status_module.get('startDateStruct', {}).get('date', None)
            primary_completion_date = status_module.get('primaryCompletionDateStruct', {}).get('date', None)
            completion_date = status_module.get('completionDateStruct', {}).get('date', None)
            study_first_submit_date = status_module.get('studyFirstSubmitDate', None)
            study_first_submit_qc_date = status_module.get('studyFirstSubmitQcDate', None)
            study_first_post_date = status_module.get('studyFirstPostDateStruct', {}).get('date', None)
            last_update_submit_date = status_module.get('lastUpdateSubmitDate', None)
            last_update_post_date = status_module.get('lastUpdatePostDateStruct', {}).get('date', None)
            oversight_has_dmc = oversight_module.get('oversightHasDmc', None)
            is_fda_regulated_drug = oversight_module.get('isFdaRegulatedDrug', None)
            is_fda_regulated_device = oversight_module.get('isFdaRegulatedDevice', None)
            is_us_export = oversight_module.get('isUsExport', None)  # This field was not present in the provided JSON
            brief_summary = description_module.get('briefSummary', None)
            detailed_description = description_module.get('detailedDescription', None)
            
            
            
            
            
        
            for org in sponsor_collaborators_module.get('collaborators', []):
                org_name = org.get('name', None)
                org_class = org.get('class', None)
                
                # Create a unique ID for each organization
                if org_name not in organization_id_map:
                    organization_id_map[org_name] = next_organization_id
                    next_organization_id += 1

                organization_id = organization_id_map[org_name]

                processed_organizations.append({
                    'organization_id': organization_id,
                    'organization_name': org_name,
                    'organization_class': org_class,
                    'study_id': study_id  # Foreign Key
                })

            processed_studies.append({
                'study_id': study_id,
                'brief_title': brief_title,
                'official_title': official_title,
                'acronym': acronym,
                'start_date': start_date,
                'primary_completion_date': primary_completion_date,
                'completion_date': completion_date,
                'study_first_submit_date': study_first_submit_date,
                'study_first_submit_qc_date': study_first_submit_qc_date,
                'study_first_post_date': study_first_post_date,
                'last_update_submit_date': last_update_submit_date,
                'last_update_post_date': last_update_post_date,
                'oversight_has_dmc': oversight_has_dmc,
                'is_fda_regulated_drug': is_fda_regulated_drug,
                'is_fda_regulated_device': is_fda_regulated_device,
                'is_us_export': is_us_export,  # This field was not present in the provided JSON
                'brief_summary': brief_summary,
                'detailed_description': detailed_description
            })
            
            
            # Process Statuses
            status_id = f"{study_id}_status"  
            status_verified_date = status_module.get('statusVerifiedDate', None)
            overall_status = status_module.get('overallStatus', None)
            has_expanded_access = status_module.get('expandedAccessInfo', {}).get('hasExpandedAccess', None)
            processed_statuses.append({
                'status_id': status_id,
                'study_id': study_id,
                'status_verified_date': status_verified_date,
                'overall_status': overall_status,
                'has_expanded_access': has_expanded_access
            })

            # Process Sponsors
            sponsor_id = str(uuid.uuid4()) 
            responsible_party_type = sponsor_collaborators_module.get('responsibleParty', {}).get('type', None)
            lead_sponsor = sponsor_collaborators_module.get('leadSponsor', {})
            sponsor_name = lead_sponsor.get('name', None)
            sponsor_class = lead_sponsor.get('class', None)
            processed_sponsors.append({
                'sponsor_id': sponsor_id,
                'study_id': study_id,
                'sponsor_name': sponsor_name,
                'sponsor_class': sponsor_class,
                'responsible_party_type': responsible_party_type
            })

            # Process Collaborators
            for collab in sponsor_collaborators_module.get('collaborators', []):
                collaborator_id = str(uuid.uuid4()) 
                collaborator_name = collab.get('name', None)
                collaborator_class = collab.get('class', None)
                processed_collaborators.append({
                    'collaborator_id': collaborator_id,
                    'sponsor_id': sponsor_id,
                    'collaborator_name': collaborator_name,
                    'collaborator_class': collaborator_class
                })

            

            # Process Design Module
            design_module = protocol_section.get('designModule', {})
            design_id = str(uuid.uuid4()) 
            processed_designs.append({
                "design_id": design_id, 
                'study_id': study_id,
                'study_type': design_module.get('studyType', None),
                'phases': design_module.get('phases', []),
                'allocation': design_module.get('designInfo', {}).get('allocation', None),
                'intervention_model': design_module.get('designInfo', {}).get('interventionModel', None),
                'intervention_model_description': design_module.get('designInfo', {}).get('interventionModelDescription', None),
                'primary_purpose': design_module.get('designInfo', {}).get('primaryPurpose', None),
                'masking': design_module.get('designInfo', {}).get('maskingInfo', {}).get('masking', None),
                'enrollment_count': design_module.get('enrollmentInfo', {}).get('count', None),
                'enrollment_type': design_module.get('enrollmentInfo', {}).get('type', None)
            })
            
            #condtion
            conditions_module = protocol_section.get('conditionsModule', {})
            for condition in conditions_module.get('conditions', []):
                condition_id=str(uuid.uuid4()) 
                processed_conditions.append({
                    "condition_id": condition_id,  # Unique ID, could use a combination of study_id and condition_name
                    "study_id": study_id,
                    "condition_name": condition
                })

            # Process Arms
            # Process Arms
            arms_interventions_module = protocol_section.get('armsInterventionsModule', {})
            for arm in arms_interventions_module.get('armGroups', []):
                arm_id = str(uuid.uuid4())  
                processed_arms.append({
                    'arm_id': arm_id,
                    'study_id': study_id,
                    'label': arm.get('label', None),
                    'type': arm.get('type', None)
                })

            # Process Interventions
            arms_interventions_module = protocol_section.get('armsInterventionsModule', {})
            for intervention in arms_interventions_module.get('interventions', []):
                intervention_id = str(uuid.uuid4())  
                intervention_id = f"{arm_id}_{intervention.get('name')}"  
                processed_interventions.append({
                    'intervention_id': intervention_id,
                    'arm_id': arm_id,
                    'type': intervention.get('type', None),
                    'name': intervention.get('name', None),
                    'description': intervention.get('description', None),
                    'other_names': intervention.get('otherNames', None)
                })

            # Process Outcomes
            outcomes_module = protocol_section.get('outcomesModule', {})
            for outcome_type in ['primaryOutcomes', 'secondaryOutcomes']:
                for outcome in outcomes_module.get(outcome_type, []):
                    outcome_id = str(uuid.uuid4()) 
                    
                    # Set outcome_type based on the key
                    if outcome_type == 'primaryOutcomes':
                        outcome_type_label = 'primary'
                    elif outcome_type == 'secondaryOutcomes':
                        outcome_type_label = 'secondary'
                        
                    processed_outcomes.append({
                        'outcome_id': outcome_id,
                        'study_id': study_id,
                        'measure': outcome.get('measure', None),
                        'time_frame': outcome.get('timeFrame', None),
                        'outcome_type': outcome_type_label 
                    })

            # Process Eligibility Module
            eligibility_module = protocol_section.get('eligibilityModule', {})
            eligibility_id = str(uuid.uuid4())  
            processed_eligibility.append({
                'eligibility_id': eligibility_id,  # Primary Key
                'study_id': study_id,              # Foreign Key
                'criteria_type': 'Inclusion/Exclusion',  # Assuming this field is fixed or derived
                'description': eligibility_module.get('eligibilityCriteria', None),  # Eligibility criteria text
                'healthy_volunteers': eligibility_module.get('healthyVolunteers', None),
                'sex': eligibility_module.get('sex', None),
                'gender_based': None,  # This field does not exist in the provided data
                'minimum_age': eligibility_module.get('minimumAge', None),
                'maximum_age': eligibility_module.get('maximumAge', None),
                'std_ages': eligibility_module.get('stdAges', [])  # Assuming this is a list
            })


            # Process Contacts
            contacts_locations_module = protocol_section.get('contactsLocationsModule', {})
            
            
            # Fetch phone and email from 'centralContacts'
            central_contact = contacts_locations_module.get('centralContacts', [])
            phone = central_contact[0].get('phone', None) if central_contact else None
            email = central_contact[0].get('email', None) if central_contact else None
            
            
            for contact in contacts_locations_module.get('overallOfficials', []): # centralContacts
                contact_id = str(uuid.uuid4()) 
                
                
                processed_contacts.append({
                    'contact_id': contact_id,
                    'study_id': study_id,
                    'name': contact.get('name', None),
                    'role': contact.get('role', None),
                    'phone': phone,
                    'email': email
                })
                
                
                
            # Process Locations
            for location in locations_module:
                location_id = str(uuid.uuid4())   
                processed_locations.append({
                    'location_id': location_id,                     
                    'study_id': study_id,                          
                    'facility': location.get('facility', None),
                    'status': location.get('status', None),          
                    'city': location.get('city', None),
                    'state': location.get('state', None),
                    'zip': location.get('zip', None),                
                    'country': location.get('country', None),
                    'lat': location.get('geoPoint', {}).get('lat', None),  
                    'lon': location.get('geoPoint', {}).get('lon', None)   
                })
                
           
            
        

        # Prepare file paths for studies data
        latest_studies_path = os.path.join(DATA_PATH, 'latest_studies_data.csv')
        latest_organizations_path = os.path.join(DATA_PATH, 'latest_organizations_data.csv')
        latest_statuses_path = os.path.join(DATA_PATH, 'latest_statuses_data.csv')
        latest_sponsors_path = os.path.join(DATA_PATH, 'latest_sponsors_data.csv')
        latest_collaborators_path = os.path.join(DATA_PATH, 'latest_collaborators_data.csv')
        latest_conditions_path = os.path.join(DATA_PATH, 'latest_conditions_data.csv')
        latest_designs_path = os.path.join(DATA_PATH, 'latest_designs_data.csv')
        latest_arms_path = os.path.join(DATA_PATH, 'latest_arms_data.csv')
        latest_interventions_path = os.path.join(DATA_PATH, 'latest_interventions_data.csv')
        latest_outcomes_path = os.path.join(DATA_PATH, 'latest_outcomes_data.csv')
        latest_eligibility_path = os.path.join(DATA_PATH, 'latest_eligibility_data.csv')
        latest_contacts_path = os.path.join(DATA_PATH, 'latest_contacts_data.csv')
        latest_locations_path = os.path.join(DATA_PATH, 'latest_locations_data.csv')
        
        # Define fieldnames for CSV files
        studies_fieldnames = ['study_id', 'brief_title', 'official_title', 'acronym', 'start_date', 
                      'primary_completion_date', 'completion_date', 'study_first_submit_date', 
                      'study_first_submit_qc_date', 'study_first_post_date', 'last_update_submit_date',
                      'last_update_post_date', 'oversight_has_dmc', 'is_fda_regulated_drug', 
                      'is_fda_regulated_device', 'is_us_export', 'brief_summary', 'detailed_description']

        organizations_fieldnames = ['organization_id', 'organization_name', 'organization_class', 'study_id']
        
        statuses_fieldnames = ['status_id', 'study_id', 'status_verified_date', 'overall_status', 'has_expanded_access']
        
        sponsors_fieldnames = ['sponsor_id', 'study_id', 'sponsor_name', 'sponsor_class', 'responsible_party_type']
        
        collaborators_fieldnames = ['collaborator_id', 'sponsor_id', 'collaborator_name', 'collaborator_class']
        
        design_fieldnames = ['design_id', 'study_id', 'study_type', 'phases', 'allocation', 'intervention_model', 
                     'intervention_model_description', 'primary_purpose', 'masking', 'enrollment_count', 
                     'enrollment_type']

        conditions_fieldnames = ['condition_id', 'study_id', 'condition_name']

        arms_fieldnames = ['arm_id', 'study_id', 'label', 'type']

        interventions_fieldnames = ['intervention_id', 'arm_id', 'type', 'name', 'description', 'other_names']

        outcomes_fieldnames = ['outcome_id', 'study_id', 'measure', 'time_frame', 'outcome_type']

        eligibility_fieldnames = ['eligibility_id', 'study_id', 'criteria_type', 'description', 'healthy_volunteers', 
                          'sex', 'gender_based', 'minimum_age', 'maximum_age', 'std_ages']

        contacts_fieldnames = ['contact_id', 'study_id', 'name', 'role', 'phone', 'email']

        locations_fieldnames = ['location_id', 'study_id', 'facility', 'status', 'city', 'state', 'zip', 'country', 
                        'lat', 'lon']





        

        # Get existing data to compare
        existing_studies_data = get_existing_data(latest_studies_path)
        existing_organizations_data = get_existing_data(latest_organizations_path)
        existing_statuses_data = get_existing_data(latest_statuses_path)
        existing_sponsors_data = get_existing_data(latest_sponsors_path)
        existing_collaborators_data = get_existing_data(latest_collaborators_path)
        existing_conditions_data = get_existing_data(latest_conditions_path)
        existing_designs_data = get_existing_data(latest_designs_path)
        existing_arms_data = get_existing_data(latest_arms_path)
        existing_interventions_data = get_existing_data(latest_interventions_path)
        existing_outcomes_data = get_existing_data(latest_outcomes_path)
        existing_eligibility_data = get_existing_data(latest_eligibility_path)
        existing_contacts_data = get_existing_data(latest_contacts_path)
        existing_locations_data = get_existing_data(latest_locations_path)
       

        # Check if the fetched data is different from existing data
        if (existing_studies_data != processed_studies or
            existing_organizations_data != processed_organizations or
            existing_statuses_data != processed_statuses or
            existing_sponsors_data != processed_sponsors or
            existing_collaborators_data != processed_collaborators or
            existing_conditions_data != processed_conditions or
            existing_designs_data != processed_designs or
            existing_arms_data != processed_arms or
            existing_interventions_data != processed_interventions or
            existing_outcomes_data != processed_outcomes or
            existing_eligibility_data != processed_eligibility or
            existing_contacts_data != processed_contacts or
            existing_locations_data != processed_locations): 

            # Archive old data if it exists
            if existing_studies_data:
                move_to_archive(latest_studies_path)
            if existing_organizations_data:
                move_to_archive(latest_organizations_path)
            if existing_statuses_data:
                move_to_archive(latest_statuses_path)
            if existing_sponsors_data:
                move_to_archive(latest_sponsors_path)
            if existing_collaborators_data:
                move_to_archive(latest_collaborators_path)
            if existing_conditions_data:
                move_to_archive(latest_conditions_path)
            if existing_designs_data:
                move_to_archive(latest_designs_path)
            if existing_arms_data:
                move_to_archive(latest_arms_path)
            if existing_interventions_data:
                move_to_archive(latest_interventions_path)
            if existing_outcomes_data:
                move_to_archive(latest_outcomes_path)
            if existing_eligibility_data:
                move_to_archive(latest_eligibility_path)
            if existing_contacts_data:
                move_to_archive(latest_contacts_path)
            if existing_locations_data:
                move_to_archive(latest_locations_path)

            # Save new data
            save_data(processed_studies, latest_studies_path, studies_fieldnames)
            save_data(processed_organizations, latest_organizations_path, organizations_fieldnames)
            save_data(processed_statuses, latest_statuses_path, statuses_fieldnames)
            save_data(processed_sponsors, latest_sponsors_path,sponsors_fieldnames)
            save_data(processed_collaborators, latest_collaborators_path,collaborators_fieldnames)
            save_data(processed_conditions, latest_conditions_path,conditions_fieldnames)
            save_data(processed_designs, latest_designs_path,design_fieldnames)
            save_data(processed_arms, latest_arms_path,arms_fieldnames)
            save_data(processed_interventions, latest_interventions_path,interventions_fieldnames)
            save_data(processed_outcomes, latest_outcomes_path,outcomes_fieldnames)
            save_data(processed_eligibility, latest_eligibility_path,eligibility_fieldnames)
            save_data(processed_contacts, latest_contacts_path,contacts_fieldnames)
            save_data(processed_locations, latest_locations_path,locations_fieldnames)

            # Upload new data
            upload_data(latest_studies_path, TARGET_URL)
            upload_data(latest_organizations_path, TARGET_URL)
            upload_data(latest_statuses_path, TARGET_URL)
            upload_data(latest_sponsors_path, TARGET_URL)
            upload_data(latest_collaborators_path, TARGET_URL)
            upload_data(latest_conditions_path, TARGET_URL)
            upload_data(latest_designs_path, TARGET_URL)
            upload_data(latest_arms_path, TARGET_URL)
            upload_data(latest_interventions_path, TARGET_URL)
            upload_data(latest_outcomes_path, TARGET_URL)
            upload_data(latest_eligibility_path, TARGET_URL)
            upload_data(latest_contacts_path, TARGET_URL)
            upload_data(latest_locations_path, TARGET_URL) 
        else:
            print("No updates in Studies data.")



    # Fetch and process version data
    # Define fieldnames for version data
    version_fieldnames = ['apiVersion', 'dataTimestamp']
    version_data = fetch_data(VERSION_URL)
    
    if version_data:
        # Ensure version_data is in the correct format
        if isinstance(version_data, dict):
            version_data = [version_data]  # Wrap in list if it's a single dictionary
        
        # Prepare file paths for version data
        latest_version_path = os.path.join(DATA_PATH, 'latest_version_data.csv')

        existing_version_data = get_existing_data(latest_version_path)

        # Check if the fetched data is different from existing data
        if existing_version_data != version_data:
            # Archive old data if it exists
            if existing_version_data:
                move_to_archive(latest_version_path)

            save_data(version_data, latest_version_path, version_fieldnames)

            upload_data(latest_version_path, TARGET_URL)
        else:
            print("No updates in version data.")


def run_scheduled_job():
    """Runs the scheduled job."""
    print("Running scheduled job...")
    process_data()

def main():
    # Run the job immediately
    run_scheduled_job()
    
    # Schedule the job to run every 1 hour
    schedule.every(1).hours.do(run_scheduled_job)
    
    # Run the scheduled jobs
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    
    
    main()