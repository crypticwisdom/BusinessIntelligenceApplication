from django.http import JsonResponse
from elasticsearch_dsl import Q
from .documents import (
    TransactionsDocument,
    SettlementMebDocument,
    SettlementDetailDocument,
)


def filterdocument(request):
    print("Starting search")

    # Define the time range filter
    # time_range_filter = Q("range", time={"gte": "2024-05-09", "lte": "2024-05-10"})

    # Create a search object with the time range filter
    s = SettlementDetailDocument.search().filter()[:90]

    # Set size parameter to retrieve the first 10 matching documents

    # Execute the search to retrieve the results
    response = s.execute()
    print(response)

    # Convert search results to a list of dictionaries
    results = [hit.to_dict() for hit in response]

    # Return JSON response
    return JsonResponse(results, safe=False)
