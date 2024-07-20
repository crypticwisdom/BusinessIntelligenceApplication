# # # signals.py

# from django.db.models.signals import pre_save, post_save
# from django.dispatch import receiver
# from django.contrib.auth.models import User
# from .models import Approval,Institution,Country,Currency

# # # @receiver(pre_save, sender=Approval)
# # # def check_status(sender, instance, **kwargs):
# # #     if instance.status != 'approved':
# # #         # Prevent saving if status is not approved
# # #         raise ValueError("Changes cannot be saved until approved by admin")

# @receiver(post_save, sender=Approval)
# def record_change(sender, instance, created, **kwargs):
#     if created:
#         pass
#     else:
#         if instance.modelName == "institution":
#             if instance.action == "create":
#                 Institution.objects.create(**instance.data)
#             else:
#                 Institution.objects.get(id=instance.fieldId)
#                 for key, value in instance.data.items():
#                     setattr(Institution, key, value)

#                 # Save the changes to the object
#                 Institution.save()

#         elif instance.modelName == "country":
#             if instance.action == "create":
#                 Country.objects.create(**instance.data)
#             else:
#                 Country.objects.get(id=instance.fieldId)
#                 for key, value in instance.data.items():
#                     setattr(Country, key, value)

#                 # Save the changes to the object
#                 Country.save()
#         elif instance.modelName == "currency":
#             if instance.action == "create":
#                 Currency.objects.create(**instance.data)
#             else:
#                 Currency.objects.get(id=instance.fieldId)
#                 Currency.objects.get(id=instance.fieldId)
#                 for key, value in instance.data.items():
#                     setattr(Currency, key, value)

#                 # Save the changes to the object
#                 Currency.save()
