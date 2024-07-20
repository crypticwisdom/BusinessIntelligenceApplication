from django.http.response import JsonResponse
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from datacore.modules.exceptions import raise_serializer_error_msg
from datacore.modules.paginations import CustomPagination
from datacore.modules.permissions import IsRiskAdmin, IsAdmin, IsPrivilegedAdmin
from datacore.modules.utils import incoming_request_checks, api_response, get_incoming_request_checks
from report.models import Transactions
from .models import InsightConfigModel, InsightModel
from .serializers import InsightAnalysisNotificationModelSerializer, InsightConfigSerializer, \
    SetInsightConfigSerializerIn
from rest_framework import status
from .utils import functions
from django.db.models import Q
from .models import InsightModel
from .serializers import InsightModelSerializer
# from .tasks import check_benchmarks


# Create your views here.

def home(request):
    return JsonResponse({"data": "Insight Module for Unified Payment Business Intelligence Application."})


class ViewDataInsightsView(APIView, CustomPagination):
    # permission_classes = [
    #       permissions.IsAuthenticated & (IsRiskAdmin | IsAdmin | IsPrivilegedAdmin)
    # ]  # permissions.IsAuthenticated
    permission_classes = [IsAuthenticated]  # permissions.IsAuthenticated

    def get(self, request):
        print('=====')
        stats, message = functions.transactions_average(request=request)

        if not stats:
            return Response(api_response(message=message.get('message'), status=False),
                            status=status.HTTP_400_BAD_REQUEST)
        return Response(data=message)


class InsightConfigView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        status_, msg = get_incoming_request_checks(request=request)
        if not status_:
            return Response(api_response(message=f"{msg}", data={}, status=False),
                            status=status.HTTP_400_BAD_REQUEST)

        instance_model_query_set = InsightModel.objects.only('name', 'admin_activated')
        name_query = request.query_params.get('name_query', None)

        query: Q = Q()

        if name_query:
            query = Q(name__icontains=name_query)

        try:
            serialized_data = InsightConfigSerializer(
                instance_model_query_set.filter(query), context={'request': request}, many=True
            ).data
        except (Exception,) as err:
            return Response(api_response(message=f"Failed to fetch insight modules: {err}", data={}, status=False),
                            status=status.HTTP_400_BAD_REQUEST)
        return Response(api_response(message=f"Retrieved Available Insights", data=serialized_data, status=True),
                        status=status.HTTP_200_OK)

    def put(self, request, slug=None):
        """
            For updating an institution's 'insight configuration'
        """
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(api_response(message=data, status=False), status=status.HTTP_400_BAD_REQUEST)

        if slug is None:
            return Response(api_response(message="Insight config 'slug' is required", status=False),
                            status=status.HTTP_400_BAD_REQUEST)

        try:
            insight_model_slug = data['insight_model_slug']
            if not insight_model_slug:
                return Response(
                    api_response(message=f"'insight_model_slug' field is required", status=False), status=status.HTTP_400_BAD_REQUEST)

        except (KeyError, Exception) as err:
            return Response(
                api_response(message=f"{err}", status=False), status=status.HTTP_400_BAD_REQUEST)
        print(insight_model_slug)

        try:
            "Collect the insight model 'id' and then get or create it"
            # print('-----')
            insight_model = InsightModel.objects.only('slug').filter(slug=insight_model_slug)
            if not insight_model.exists():
                return Response(
                    api_response(message=f"Insight Model with slug '{insight_model_slug}' does not exists",
                                 status=False), status=status.HTTP_400_BAD_REQUEST)
            insight_model = insight_model.get(slug=insight_model_slug)
            print(insight_model, "=======")
            insight_config_instance_check = InsightConfigModel.objects.filter(insight=insight_model, slug=slug)

            if not insight_config_instance_check.exists():
                return Response(
                    api_response(message=f"Insight Config Model Instance with slug '{slug}' does not have an", status=False), status=status.HTTP_400_BAD_REQUEST)

            # print(insight_model, '================')
            insight_config_instance = InsightConfigModel.objects.get(slug=slug)
        except (Exception,) as err:
            return Response(
                api_response(message=f"{err}", status=False), status=status.HTTP_400_BAD_REQUEST)

        serializer = SetInsightConfigSerializerIn(data=data, instance=insight_config_instance,
                                                  context={'request': request})
        serializer.is_valid() or raise_serializer_error_msg(errors=serializer.errors)
        serializer.update(instance=serializer.instance, validated_data=serializer.validated_data)
        return Response(
            api_response(message="Successfully updated insight configuration", status=False, data=serializer.data),
            status=status.HTTP_200_OK)




# class BenchmarkListCreateAPIView(APIView):
#     def get(self, request):
#         benchmarks = Benchmark.objects.all()
#         serializer = BenchmarkSerializer(benchmarks, many=True)
#         return Response(serializer.data)

#     def post(self, request):
#         serializer = BenchmarkSerializer(data=request.data)
#         if serializer.is_valid():
#             serializer.save()
#             return Response(serializer.data, status=status.HTTP_201_CREATED)
#         return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# class BenchmarkDetailAPIView(APIView):
#     def get_object(self, pk):
#         try:
#             return Benchmark.objects.get(pk=pk)
#         except Benchmark.DoesNotExist:
#             raise False

#     def get(self, request, pk):
#         benchmark = self.get_object(pk)
#         serializer = BenchmarkSerializer(benchmark)
#         return Response(serializer.data)

#     def put(self, request, pk):
#         benchmark = self.get_object(pk)
#         serializer = BenchmarkSerializer(benchmark, data=request.data)
#         if serializer.is_valid():
#             serializer.save()
#             return Response(serializer.data)
#         return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

#     def delete(self, request, pk):
#         benchmark = self.get_object(pk)
#         benchmark.delete()
#         return Response(status=status.HTTP_204_NO_CONTENT)

# class InsightModelListCreateAPIView(APIView):
#     def get(self, request):
#         insights = InsightModel.objects.all()
#         serializer = InsightModelSerializer(insights, many=True)
#         return Response(serializer.data)

#     def post(self, request):
#         serializer = InsightModelSerializer(data=request.data)
#         if serializer.is_valid():
#             serializer.save()
#             return Response(serializer.data, status=status.HTTP_201_CREATED)
#         return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# class InsightModelDetailAPIView(APIView):
#     def get_object(self, pk):
#         try:
#             return InsightModel.objects.get(pk=pk)
#         except InsightModel.DoesNotExist:
#             raise False

#     def get(self, request, pk):
#         insight = self.get_object(pk)
#         serializer = InsightModelSerializer(insight)
#         return Response(serializer.data)

#     def put(self, request, pk):
#         insight = self.get_object(pk)
#         serializer = InsightModelSerializer(insight, data=request.data)
#         if serializer.is_valid():
#             serializer.save()
#             return Response(serializer.data)
#         return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

#     def delete(self, request, pk):
#         insight = self.get_object(pk)
#         insight.delete()
#         return Response(status=status.HTTP_204_NO_CONTENT)

# class BenchmarkCheckAPIView(APIView):
#     def post(self, request):
#         # Perform benchmark checks
#         check_benchmarks()
#         return Response({"message": "Benchmark checks performed successfully"}, status=status.HTTP_200_OK)
