# Generated by Django 2.2.2 on 2019-06-21 06:22

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('eagle', '0001_initial'),
    ]

    operations = [
        migrations.RenameField(
            model_name='numberofexecutives',
            old_name='base',
            new_name='ground',
        ),
    ]
