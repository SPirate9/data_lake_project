from django.db import models
from django.contrib.auth.models import User

class AccessLog(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    datetime = models.DateTimeField(auto_now_add=True)
    endpoint = models.CharField(max_length=255)
    request_body = models.TextField()
    datetime = models.DateTimeField(auto_now_add=True)
    folder_path = models.CharField(max_length=255, blank=True, null=True)