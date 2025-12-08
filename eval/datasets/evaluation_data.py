"""
Evaluation dataset for dbxmetagen prompt testing.

This module creates ground truth datasets for evaluating metadata generation
across different modes (comment, pi, domain).
"""

import pandas as pd
from typing import Dict, List, Any


def create_comment_mode_examples() -> List[Dict[str, Any]]:
    """Create evaluation examples for comment mode."""
    return [
        {
            "request_id": "customer_pii_comment_1",
            "inputs": {
                "table_name": "customer_profiles",
                "columns": [
                    "customer_id",
                    "full_name",
                    "email",
                    "phone",
                    "created_date",
                ],
                "column_types": {
                    "customer_id": "INT",
                    "full_name": "STRING",
                    "email": "STRING",
                    "phone": "STRING",
                    "created_date": "TIMESTAMP",
                },
                "sample_data": [
                    {
                        "customer_id": 1,
                        "full_name": "John Smith",
                        "email": "john.smith@example.com",
                        "phone": "555-0123",
                        "created_date": "2024-01-15",
                    },
                    {
                        "customer_id": 2,
                        "full_name": "Jane Doe",
                        "email": "jane.doe@example.com",
                        "phone": "555-0124",
                        "created_date": "2024-01-16",
                    },
                    {
                        "customer_id": 3,
                        "full_name": "Bob Johnson",
                        "email": "bob.j@example.com",
                        "phone": "555-0125",
                        "created_date": "2024-01-17",
                    },
                ],
                "metadata": {
                    "row_count": 1000,
                    "null_counts": {
                        "customer_id": 0,
                        "full_name": 5,
                        "email": 3,
                        "phone": 50,
                        "created_date": 0,
                    },
                },
                "mode": "comment",
            },
            "ground_truth": {
                "table": "Customer profiles table containing personal identification information and contact details for registered customers. Includes unique customer identifiers, full names, email addresses, phone numbers, and account creation timestamps. Primary key is customer_id.",
                "columns": [
                    "customer_id",
                    "full_name",
                    "email",
                    "phone",
                    "created_date",
                ],
                "column_contents": [
                    "Unique integer identifier assigned to each customer upon registration. Serves as the primary key for the customer_profiles table with no null values.",
                    "Customer's full legal name as provided during registration. Contains first and last name. May have occasional null values (5 out of 1000 records) for incomplete registrations.",
                    "Customer's primary email address used for account communications and login. Standard email format. Contains PII and has minimal null values (3 out of 1000).",
                    "Customer's contact phone number. May be in various formats. Contains PII with higher null rate (50 out of 1000) as phone is optional during registration.",
                    "Timestamp indicating when the customer account was created. No null values as this is automatically set during registration.",
                ],
            },
        },
        {
            "request_id": "sales_transactions_comment_1",
            "inputs": {
                "table_name": "sales_transactions",
                "columns": [
                    "transaction_id",
                    "customer_id",
                    "product_id",
                    "amount",
                    "transaction_date",
                ],
                "column_types": {
                    "transaction_id": "STRING",
                    "customer_id": "INT",
                    "product_id": "STRING",
                    "amount": "DECIMAL(10,2)",
                    "transaction_date": "DATE",
                },
                "sample_data": [
                    {
                        "transaction_id": "TXN001",
                        "customer_id": 1,
                        "product_id": "PRD123",
                        "amount": 99.99,
                        "transaction_date": "2024-11-15",
                    },
                    {
                        "transaction_id": "TXN002",
                        "customer_id": 2,
                        "product_id": "PRD456",
                        "amount": 149.50,
                        "transaction_date": "2024-11-16",
                    },
                    {
                        "transaction_id": "TXN003",
                        "customer_id": 1,
                        "product_id": "PRD789",
                        "amount": 29.99,
                        "transaction_date": "2024-11-17",
                    },
                ],
                "metadata": {
                    "row_count": 50000,
                    "null_counts": {
                        "transaction_id": 0,
                        "customer_id": 0,
                        "product_id": 0,
                        "amount": 0,
                        "transaction_date": 0,
                    },
                },
                "mode": "comment",
            },
            "ground_truth": {
                "table": "Sales transactions table recording all customer purchases. Contains transaction identifiers, customer references, product references, monetary amounts, and transaction dates. Foreign keys to customer_profiles and products tables. No null values as all fields are required for valid transactions.",
                "columns": [
                    "transaction_id",
                    "customer_id",
                    "product_id",
                    "amount",
                    "transaction_date",
                ],
                "column_contents": [
                    "Unique string identifier for each transaction. Format appears to be 'TXN' followed by sequential numbers. Primary key with no null values.",
                    "Foreign key referencing customer_profiles.customer_id. Links transaction to the customer who made the purchase. Required field with no nulls.",
                    "Foreign key referencing products table. String identifier for the purchased product. Format appears to be 'PRD' followed by numbers. No null values.",
                    "Transaction amount in decimal format with 2 decimal places. Represents the total cost of the transaction. No negative values or nulls observed.",
                    "Date when the transaction occurred. Standard date format. Required field for all transactions with no null values.",
                ],
            },
        },
        {
            "request_id": "employee_sensitive_comment_1",
            "inputs": {
                "table_name": "employee_records",
                "columns": ["employee_id", "ssn", "salary", "department", "hire_date"],
                "column_types": {
                    "employee_id": "INT",
                    "ssn": "STRING",
                    "salary": "DECIMAL(10,2)",
                    "department": "STRING",
                    "hire_date": "DATE",
                },
                "sample_data": [
                    {
                        "employee_id": 101,
                        "ssn": "123-45-6789",
                        "salary": 75000.00,
                        "department": "Engineering",
                        "hire_date": "2020-03-15",
                    },
                    {
                        "employee_id": 102,
                        "ssn": "987-65-4321",
                        "salary": 82000.00,
                        "department": "Sales",
                        "hire_date": "2019-07-22",
                    },
                    {
                        "employee_id": 103,
                        "ssn": "456-78-9012",
                        "salary": 68000.00,
                        "department": "Marketing",
                        "hire_date": "2021-01-10",
                    },
                ],
                "metadata": {
                    "row_count": 500,
                    "null_counts": {
                        "employee_id": 0,
                        "ssn": 0,
                        "salary": 0,
                        "department": 2,
                        "hire_date": 0,
                    },
                },
                "mode": "comment",
            },
            "ground_truth": {
                "table": "Employee records table containing sensitive HR information including personal identification numbers, compensation data, and employment details. Contains highly sensitive PII (SSN) and confidential salary information. Primary key is employee_id with minimal null values across most fields.",
                "columns": ["employee_id", "ssn", "salary", "department", "hire_date"],
                "column_contents": [
                    "Unique integer identifier for each employee. Primary key for employee_records table. No null values.",
                    "Employee's Social Security Number in XXX-XX-XXXX format. Highly sensitive PII requiring strict access controls and encryption. No null values as required for all employees.",
                    "Employee's annual salary in decimal format. Confidential compensation information with 2 decimal places. No null values.",
                    "Department assignment for the employee. String value indicating organizational unit. Has 2 null values out of 500, possibly for unassigned new hires.",
                    "Date when employee was hired. Standard date format used for calculating tenure and benefits eligibility. No null values.",
                ],
            },
        },
    ]


def create_pi_mode_examples() -> List[Dict[str, Any]]:
    """Create evaluation examples for PI mode."""
    return [
        {
            "request_id": "customer_pii_pi_1",
            "inputs": {
                "table_name": "customer_profiles",
                "columns": [
                    "customer_id",
                    "full_name",
                    "email",
                    "phone",
                    "created_date",
                ],
                "column_types": {
                    "customer_id": "INT",
                    "full_name": "STRING",
                    "email": "STRING",
                    "phone": "STRING",
                    "created_date": "TIMESTAMP",
                },
                "sample_data": [
                    {
                        "customer_id": 1,
                        "full_name": "John Smith",
                        "email": "john.smith@example.com",
                        "phone": "555-0123",
                        "created_date": "2024-01-15",
                    },
                    {
                        "customer_id": 2,
                        "full_name": "Jane Doe",
                        "email": "jane.doe@example.com",
                        "phone": "555-0124",
                        "created_date": "2024-01-16",
                    },
                ],
                "mode": "pi",
            },
            "ground_truth": {
                "table": {"classification": "PII", "type": "CUSTOMER_DATA"},
                "columns": [
                    "customer_id",
                    "full_name",
                    "email",
                    "phone",
                    "created_date",
                ],
                "classification": ["None", "PII", "PII", "PII", "None"],
                "type": ["IDENTIFIER", "NAME", "EMAIL", "PHONE", "TEMPORAL"],
            },
        },
        {
            "request_id": "employee_sensitive_pi_1",
            "inputs": {
                "table_name": "employee_records",
                "columns": ["employee_id", "ssn", "salary", "department", "hire_date"],
                "column_types": {
                    "employee_id": "INT",
                    "ssn": "STRING",
                    "salary": "DECIMAL(10,2)",
                    "department": "STRING",
                    "hire_date": "DATE",
                },
                "sample_data": [
                    {
                        "employee_id": 101,
                        "ssn": "123-45-6789",
                        "salary": 75000.00,
                        "department": "Engineering",
                        "hire_date": "2020-03-15",
                    },
                    {
                        "employee_id": 102,
                        "ssn": "987-65-4321",
                        "salary": 82000.00,
                        "department": "Sales",
                        "hire_date": "2019-07-22",
                    },
                ],
                "mode": "pi",
            },
            "ground_truth": {
                "table": {"classification": "PII", "type": "EMPLOYEE_DATA"},
                "columns": ["employee_id", "ssn", "salary", "department", "hire_date"],
                "classification": ["None", "PII", "SENSITIVE", "None", "None"],
                "type": ["IDENTIFIER", "SSN", "FINANCIAL", "ORGANIZATION", "TEMPORAL"],
            },
        },
        {
            "request_id": "sales_transactions_pi_1",
            "inputs": {
                "table_name": "sales_transactions",
                "columns": [
                    "transaction_id",
                    "customer_id",
                    "product_id",
                    "amount",
                    "transaction_date",
                ],
                "column_types": {
                    "transaction_id": "STRING",
                    "customer_id": "INT",
                    "product_id": "STRING",
                    "amount": "DECIMAL(10,2)",
                    "transaction_date": "DATE",
                },
                "sample_data": [
                    {
                        "transaction_id": "TXN001",
                        "customer_id": 1,
                        "product_id": "PRD123",
                        "amount": 99.99,
                        "transaction_date": "2024-11-15",
                    },
                    {
                        "transaction_id": "TXN002",
                        "customer_id": 2,
                        "product_id": "PRD456",
                        "amount": 149.50,
                        "transaction_date": "2024-11-16",
                    },
                ],
                "mode": "pi",
            },
            "ground_truth": {
                "table": {"classification": "None", "type": "TRANSACTIONAL"},
                "columns": [
                    "transaction_id",
                    "customer_id",
                    "product_id",
                    "amount",
                    "transaction_date",
                ],
                "classification": ["None", "None", "None", "None", "None"],
                "type": [
                    "IDENTIFIER",
                    "FOREIGN_KEY",
                    "FOREIGN_KEY",
                    "FINANCIAL",
                    "TEMPORAL",
                ],
            },
        },
    ]


def create_eval_dataset(modes: List[str] = None) -> pd.DataFrame:
    """
    Create evaluation dataset for dbxmetagen prompt testing.

    Args:
        modes: List of modes to include. If None, includes all modes.
               Options: ['comment', 'pi', 'domain']

    Returns:
        DataFrame compatible with mlflow.evaluate()
    """
    if modes is None:
        modes = ["comment", "pi"]

    all_examples = []

    if "comment" in modes:
        all_examples.extend(create_comment_mode_examples())

    if "pi" in modes:
        all_examples.extend(create_pi_mode_examples())

    return pd.DataFrame(all_examples)


if __name__ == "__main__":
    # Test dataset creation
    df = create_eval_dataset()
    print(f"Created evaluation dataset with {len(df)} examples")
    print(f"\nModes: {df['inputs'].apply(lambda x: x['mode']).unique()}")
    print(f"\nExample request IDs:")
    for rid in df["request_id"].head():
        print(f"  - {rid}")
