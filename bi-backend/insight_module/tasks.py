# # benchmark_checker.py
# from datetime import datetime, timedelta
# from report.models import Transactions
# from .models import Benchmark

# def check_benchmarks():
#     # Get current date
#     today = datetime.now().date()

#     benchmarks = Benchmark.objects.filter(active=True)

#     for benchmark in benchmarks:
#         if benchmark.daily_benchmark:
#             daily_transactions = Transactions.objects.filter(date=today)
#             for transaction in daily_transactions:
#                 if transaction.amount > benchmark.daily_benchmark:
#                     # TODO:firebase notice
#                     print(f"Transaction exceeded daily benchmark: {transaction}")

#         if benchmark.weekly_benchmark:
#             week_start = today - timedelta(days=today.weekday())
#             weekly_transactions = Transactions.objects.filter(date__gte=week_start)
#             weekly_total = sum(transaction.amount for transaction in weekly_transactions)
#             if weekly_total > benchmark.weekly_benchmark:
#                 # TODO: firebase notice
#                 print("Weekly benchmark exceeded")

#         # Check monthly benchmarks
#         if benchmark.monthly_benchmark:
#             month_start = today.replace(day=1)
#             monthly_transactions = Transactions.objects.filter(date__gte=month_start)
#             monthly_total = sum(transaction.amount for transaction in monthly_transactions)
#             if monthly_total > benchmark.monthly_benchmark:
#                 # firebase notice
#                 print("Monthly benchmark exceeded")

# if __name__ == "__main__":
#     check_benchmarks()
