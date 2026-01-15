import os
from typing import Optional, Dict, Any, List

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

from app.database import get_dynamodb

aws_default_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")


class DynamoDBManager:
    def __init__(
            self,
            region_name: str = aws_default_region,
            dynamodb_resource: Optional[boto3.resource] = None
    ):
        """
        Initialize the DynamoDBManager.

        :param region_name: AWS region name.
        :param dynamodb_resource: Optional DynamoDB resource for dependency injection.
        """
        self.region_name = region_name
        self.dynamodb = dynamodb_resource or get_dynamodb(self.region_name)

    def get_table(self, table_name: str) -> boto3.resource:
        """
        Get a DynamoDB table resource.

        This method retrieves a reference to the specified DynamoDB table
        using the boto3 resource interface.

        :param table_name: The name of the DynamoDB table to retrieve.
        :return: A boto3 DynamoDB Table resource object.
        """
        try:
            table = self.dynamodb.Table(table_name)
            return table
        except (NoCredentialsError, PartialCredentialsError, ClientError, Exception) as e:
            print(f"Error retrieving table: {e}")
            return None

    def put_item(self, table_name: str, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Insert an item into the DynamoDB table.

        :param table_name: Name of the DynamoDB table.
        :param item: Item to be inserted.
        :return: Response from DynamoDB.
        """
        table = self.get_table(table_name)
        try:
            response = table.put_item(Item=item)
            return response

        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Error inserting item: {e}")
        return None

    def get_item(self, table_name: str, key: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Get an item from the DynamoDB table.

        :param table_name: Name of the DynamoDB table.
        :param key: Key of the item to retrieve.
        :return: Retrieved item.
        """
        table = self.get_table(table_name)
        try:
            response = table.get_item(Key=key)
            return response.get('Item', None)
        except Exception as e:
            print(f"Error getting item: {e}")
        return None

    def update_item(
            self,
            table_name: str,
            key: Dict[str, Any],
            update_expression: str,
            expression_attribute_values: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Update an item in the DynamoDB table.
        Note: You cannot update the value of the partition key (or the combination
        of partition key and sort key in case of a composite primary key) for an existing item.

        :param table_name: Name of the DynamoDB table.
        :param key: Key of the item to update.
        :param update_expression: Update expression for the item.
        :param expression_attribute_values: Values for the update expression.
        :return: Response from DynamoDB.
        """
        table = self.get_table(table_name)
        try:
            response = table.update_item(
                Key=key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues="UPDATED_NEW"
            )
            return response
        except Exception as e:
            print(f"Error updating item: {e}")
        return None

    def delete_item(self, table_name: str, key: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Delete an item from the DynamoDB table.

        :param table_name: Name of the DynamoDB table.
        :param key: Key of the item to delete.
        :return: Response from DynamoDB.
        """
        table = self.get_table(table_name)
        try:
            response = table.delete_item(Key=key)
            return response
        except Exception as e:
            print(f"Error deleting item: {e}")
        return None

    def query_by_keys(self, table_name: str, condition_expression, filter_expression=None,
                      expression_attribute_names=None, expression_attribute_values=None) -> List[Dict[str, Any]]:
        """
        Query the DynamoDB table by a condition expression and handle pagination.

        :param table_name: Name of the DynamoDB table.
        :param condition_expression: Condition expression for querying the table.
        :param filter_expression: Optional filter expression for querying the table.
        :param expression_attribute_names: Optional expression attribute names for querying the table.
        :param expression_attribute_values: Optional expression attribute values for querying the table.
        :return: List of items matching the condition expression.
        """
        table = self.get_table(table_name)
        return self._query_with_pagination(table, condition_expression, filter_expression,
                                           expression_attribute_names, expression_attribute_values)

    @staticmethod
    def _query_with_pagination(table, condition_expression, filter_expression=None,
                               expression_attribute_names=None, expression_attribute_values=None):
        """
        Query the DynamoDB table with pagination.

        :param table: DynamoDB Table resource.
        :param condition_expression: KeyConditionExpression for the query.
        :param filter_expression: Optional filter expression for the query.
        :param expression_attribute_names: Optional expression attribute names for querying the table.
        :param expression_attribute_values: Optional expression attribute values for querying the table.
        :return: List of items matching the key conditions.
        """
        try:
            query_params = {
                "KeyConditionExpression": condition_expression,
            }
            if filter_expression:
                query_params["FilterExpression"] = filter_expression
            if expression_attribute_names:
                query_params["ExpressionAttributeNames"] = expression_attribute_names
            if expression_attribute_values:
                query_params["ExpressionAttributeValues"] = expression_attribute_values

            response = table.query(**query_params)
            items = response['Items']

            while 'LastEvaluatedKey' in response:
                query_params["ExclusiveStartKey"] = response['LastEvaluatedKey']
                response = table.query(**query_params)
                items.extend(response['Items'])

            return items
        except Exception as e:
            print(f"Error querying items: {e}")
            return []

    def get_items_with_id_and_sub_id_prefix(self, table_name, id, sub_id_prefix: str) -> list:
        table = self.get_table(table_name)
        response = table.query(
            KeyConditionExpression=Key('id').eq(id)
                                   & Key('sub_id').begins_with(sub_id_prefix)
        )
        return response.get("Items", [])


def get_dynamodb_manager() -> DynamoDBManager:
    return DynamoDBManager()
