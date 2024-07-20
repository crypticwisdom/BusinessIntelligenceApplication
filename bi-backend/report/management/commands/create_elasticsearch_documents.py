from django.core.management.base import BaseCommand
from django_elasticsearch_dsl.registries import registry

class Command(BaseCommand):
    help = 'Create Elasticsearch documents for Django models'

    def handle(self, *args, **options):
        # Iterate over all registered Django models
        for model in registry.get_models():
            # Check if the model has a corresponding Elasticsearch document
            if registry.document_exists(model):
                # Get the Elasticsearch document class
                doc_class = registry.get_document(model)
                # Generate the Elasticsearch document file content
                document_content = generate_document_content(doc_class)
                # Write the content to a file
                file_name = f'{model.__name__}Document.py'
                with open(file_name, 'w') as file:
                    file.write(document_content)

def generate_document_content(doc_class):
    # Generate the content of the Elasticsearch document file
    content = f"""from django_elasticsearch_dsl import Document, fields
from django_elasticsearch_dsl.registries import registry
from .models import {doc_class.django.model.__name__}

@registry.register_document
class {doc_class.__name__}(Document):
    class Index:
        name = '{doc_class.Index.name}'
        settings = {doc_class.Index.settings}

    class Django:
        model = {doc_class.django.model.__name__}

    # Define fields you want to index
"""
    # Add fields to the content
    for field in doc_class().get_fields().keys():
        content += f"    {field} = fields.TextField()\n"
    return content
