from airflow.decorators import task

import json
import os

from bs4 import BeautifulSoup
import re
def clean_text(texte):
    if texte is not None:
        # Supprimer les balises HTML
        text_without_html = BeautifulSoup(texte, 'html.parser').get_text()

        # Supprimer les caractères spéciaux
        cleaned_text = re.sub(r'[^a-zA-Z0-9\s]', '', text_without_html)

        return cleaned_text
    else:
        return None

def transform_json(json_data):
    # Créer un nouveau dictionnaire avec les clés sélectionnées et renommées
    output_data = {
        "job": {
            "title": json_data.get("title", None),
            "industry": json_data.get("industry", None),
            "description": json_data.get("description", None),
            "employment_type": json_data.get("employmentType", None),
            "date_posted": json_data.get("datePosted", None)
        },
        "company": {
            "name": json_data.get("hiringOrganization", {}).get("name", None),
            "link": json_data.get("hiringOrganization", {}).get("sameAs", None)
        },
        "education": {
            "required_credential": json_data.get("educationRequirements", {}).get("credentialCategory", None)
        },
        "experience": {
            "months_of_experience": None,
            "seniority_level": None
        },
        "salary": {
            "currency": json_data.get("estimatedSalary", {}).get("currency", None),
            "min_value": json_data.get("estimatedSalary", {}).get("value", {}).get("minValue", None),
            "max_value": json_data.get("estimatedSalary", {}).get("value", {}).get("maxValue", None),
            "unit": json_data.get("estimatedSalary", {}).get("value", {}).get("unitText", None)
        },
        "location": {
            "country": json_data.get("jobLocation", {}).get("address", {}).get("addressCountry", None),
            "locality": json_data.get("jobLocation", {}).get("address", {}).get("addressLocality", None),
            "region": json_data.get("jobLocation", {}).get("address", {}).get("addressRegion", None),
            "postal_code": json_data.get("jobLocation", {}).get("address", {}).get("postalCode", None),
            "street_address": json_data.get("jobLocation", {}).get("address", {}).get("streetAddress", None),
            "latitude": json_data.get("jobLocation", {}).get("latitude", None),
            "longitude": json_data.get("jobLocation", {}).get("longitude", None)
        },
    }
    
    # Extract values from experienceRequirements
    experience_requirements = json_data.get("experienceRequirements")

    if isinstance(experience_requirements, str):
        # If it's a string, use its value
        output_data["experience"]["months_of_experience"] = experience_requirements
    elif isinstance(experience_requirements, dict):
        # If it's a dictionary, extract values
        output_data["experience"]["months_of_experience"] = experience_requirements.get("monthsOfExperience", None)
        
    # Extract seniority_level from title
    title = output_data["job"]["title"]
    if title is not None:
        seniority_indicators = ["Senior", "Lead", "Principal", "Manager", "Sr."]

        for indicator in seniority_indicators:
            if indicator in title:
                output_data["experience"]["seniority_level"] = indicator
                break

    # Clean description
    description = output_data["job"]["description"]
    output_data["job"]["description"] = clean_text(description)

    return output_data


@task()
def transform():
    """Clean and convert extracted elements to json."""

    extracted_folder_path = 'staging/extracted'
    transformed_folder_path = 'staging/transformed'

    # Créer le dossier staging/transformed s'il n'existe pas
    os.makedirs(transformed_folder_path, exist_ok=True)

    for filename in os.listdir(extracted_folder_path):
        if filename.endswith('.txt'):
            input_file_path = os.path.join(extracted_folder_path, filename)
            
            # Lire le contenu du fichier texte
            with open(input_file_path, 'r', encoding='utf-8') as file:
                text_content = file.read()

            # Convertir le texte en objet JSON
            try:
                json_data = json.loads(text_content)
            except json.JSONDecodeError as e:
                print(f"Erreur de décodage JSON : {e}")

            # Appeler la fonction transform_json avec json_data
            output_data = transform_json(json_data)

            # Créer le chemin de sortie pour le fichier JSON
            output_file_path = os.path.join(transformed_folder_path, f"{filename.replace('extracted', 'transformed')[:-4]}.json")

            # Écrire les données dans le fichier JSON
            with open(output_file_path, 'w', encoding='utf-8') as json_file:
                json.dump(output_data, json_file, ensure_ascii=False, indent=2)
