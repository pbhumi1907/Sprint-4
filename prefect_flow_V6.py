# -*- coding: utf-8 -*-
"""
Created on Wed Apr  9 11:54:37 2025

@author: Bhumika
"""

from prefect import flow, task
import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.styles import getSampleStyleSheet
import asyncio


# Suppress Prefect logs (set logging level to WARNING or higher)
logging.basicConfig(level=logging.WARNING)

# ---------- Read CSV Task ----------
@task
def read_csv(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)

# ---------- Data Validation Task ----------
@task
def validate_data(customers, orders, products, sales):
    validation_report = []

    for name, df in [("customers", customers), ("orders", orders), ("products", products), ("sales", sales)]:
        if df.isnull().values.any():
            missing_columns = df.columns[df.isnull().any()].tolist()
            validation_report.append(f"{name} contains missing values in columns: {', '.join(missing_columns)}")
        else:
            validation_report.append(f"{name} has no missing values.")

    if not pd.api.types.is_numeric_dtype(products["price"]):
        validation_report.append("Product price must be numeric.")
    else:
        validation_report.append("Product price is valid.")

    if not pd.api.types.is_numeric_dtype(sales["quantity_sold"]):
        validation_report.append("Sales quantity must be numeric.")
    else:
        validation_report.append("Sales quantity is valid.")

    if customers["customer_id"].duplicated().any():
        validation_report.append("Duplicate customer IDs found.")
    else:
        validation_report.append("Customer IDs are unique.")

    if products["product_id"].duplicated().any():
        validation_report.append("Duplicate product IDs found.")
    else:
        validation_report.append("Product IDs are unique.")

    if sales["sale_id"].duplicated().any():
        validation_report.append("Duplicate sale IDs found.")
    else:
        validation_report.append("Sale IDs are unique.")

    # Print validation report to console
    for line in validation_report:
        print(line)

    # Save validation report to file
    with open('validation_report.txt', 'w') as f:
        for line in validation_report:
            f.write(line + "\n")

    print("✅ Data validation report generated.")
    return validation_report

# ---------- Merge Data Task ----------
@task
def merge_data(customers, orders, products) -> pd.DataFrame:
    merged = orders.merge(customers, on="customer_id", how="left")
    merged = merged.merge(products, on="product_id", how="left")
    
    # Save merged data as CSV file
    merged.to_csv("merged_data.csv", index=False)
    print("✅ Merged data saved as 'merged_data.csv'.")
    
    # Print the merged data to the console
    print(merged.head())  # Display the first few rows of the merged DataFrame
    
    return merged

# ---------- MySQL Insert Task ----------
@task
def insert_to_mysql(data: pd.DataFrame, table: str):
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="ecommerce"
    )
    cursor = connection.cursor()

    required_columns = {
        "orders": ['order_id', 'customer_id', 'product_id', 'quantity', 'order_date'],
        "customers": ['customer_id', 'name', 'email'],
        "products": ['product_id', 'product_name', 'price'],
        "sales": ['sale_id', 'product_id', 'quantity_sold', 'sale_date']
    }

    missing_columns = [col for col in required_columns[table] if col not in data.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in {table}: {', '.join(missing_columns)}")

    validation_report = []

    for _, row in data.iterrows():
        if table == "orders":
            cursor.execute(""" 
                INSERT INTO orders (order_id, customer_id, product_id, quantity, order_date) 
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE quantity=VALUES(quantity), order_date=VALUES(order_date)
            """, tuple(row[['order_id', 'customer_id', 'product_id', 'quantity', 'order_date']]))

        elif table == "customers":
            cursor.execute(""" 
                INSERT INTO customers (customer_id, name, email) 
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE name=VALUES(name), email=VALUES(email)
            """, tuple(row[['customer_id', 'name', 'email']]))

        elif table == "products":
            cursor.execute(""" 
                INSERT INTO products (product_id, product_name, price) 
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE product_name=VALUES(product_name), price=VALUES(price)
            """, tuple(row[['product_id', 'product_name', 'price']]))

        elif table == "sales":
            cursor.execute(""" 
                INSERT INTO sales (sale_id, product_id, quantity_sold, sale_date) 
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE quantity_sold=VALUES(quantity_sold), sale_date=VALUES(sale_date)
            """, tuple(row[['sale_id', 'product_id', 'quantity_sold', 'sale_date']]))


            validation_report.append(f"Row inserted/updated in {table} for ID {row.iloc[0]}")

    connection.commit()
    cursor.close()
    connection.close()
    return validation_report

# ---------- Report Generation Task ----------
@task
def generate_reports(sales, products, customers, orders):
    merged = sales.merge(products, on='product_id', how='left')
    merged['revenue'] = merged['quantity_sold'] * merged['price']

    revenue_by_product = merged.groupby('product_name')['revenue'].sum().reset_index()
    total_revenue = merged['revenue'].sum()

    top_selling = merged.groupby('product_name').agg({
        'quantity_sold': 'sum',
        'revenue': 'sum'
    }).sort_values(by='quantity_sold', ascending=False).head(5).reset_index()

    customer_segmentation = pd.DataFrame({
        'segment': ['High Value', 'Medium Value', 'Low Value'],
        'customer_count': [50, 120, 300],
        'revenue': [50000, 30000, 10000]
    })

    # --- Total Revenue by Product ---
    plt.figure(figsize=(8,6))
    colors = sns.color_palette("husl", len(revenue_by_product))
    bars = plt.barh(revenue_by_product['product_name'], revenue_by_product['revenue'], color=colors)
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 100, bar.get_y() + bar.get_height()/2, f"${width:,.2f}", va='center')
    plt.title('Total Revenue by Product')
    plt.tight_layout()
    plt.savefig('revenue_by_product.png')
    plt.show()

    # --- Total Revenue (Single Bar) ---
    plt.figure(figsize=(6,6))
    plt.bar(['Total Revenue'], [total_revenue], color='lightcoral')
    plt.text(0, total_revenue + 500, f"${total_revenue:,.2f}", ha='center')
    plt.title('Total Revenue')
    plt.ylabel('Revenue ($)')
    plt.tight_layout()
    plt.savefig('total_revenue.png')
    plt.close()

    # --- Total Revenue by Order Date ---
    revenue_by_date = orders.merge(products, on='product_id', how='left')
    revenue_by_date['revenue'] = revenue_by_date['quantity'] * revenue_by_date['price']
    grouped = revenue_by_date.groupby('order_date')['revenue'].sum().reset_index()

    plt.figure(figsize=(10,6))
    plt.plot(grouped['order_date'], grouped['revenue'], marker='o', color='teal')
    for i, row in grouped.iterrows():
        plt.text(row['order_date'], row['revenue'] + 200, f"${row['revenue']:,.0f}", ha='center', fontsize=8)
    plt.title('Total Revenue by Order Date')
    plt.xlabel('Order Date')
    plt.ylabel('Revenue ($)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('total_revenue_by_date.png')
    plt.show()

    # --- Top Selling Products ---
    plt.figure(figsize=(8,6))
    bars = plt.barh(top_selling['product_name'], top_selling['quantity_sold'], color=sns.color_palette("Set2", len(top_selling)))
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 2, bar.get_y() + bar.get_height()/2, f"{width}", va='center')
    plt.title('Top-Selling Products by Quantity')
    plt.tight_layout()
    plt.savefig('top_selling_products.png')
    plt.show()

    # --- Customer Segmentation Pie ---
    plt.figure(figsize=(8, 6))
    plt.pie(customer_segmentation['revenue'], labels=customer_segmentation['segment'], autopct='%1.1f%%', startangle=140)
    plt.title('Revenue Contribution by Customer Segment')
    plt.tight_layout()
    plt.savefig('customer_segmentation_revenue_pie.png')
    plt.show()

    # --- PDF Report ---
    create_pdf(revenue_by_product, total_revenue, grouped, top_selling, customer_segmentation)
    



def create_pdf(revenue_by_product, total_revenue, grouped, top_selling, customer_segmentation):
    # Set up the PDF canvas and page size
    pdf_path = "summary_report_with_charts.pdf"
    c = canvas.Canvas(pdf_path, pagesize=letter)
    width, height = letter  # Dimensions of the page

    # Title and total revenue
    c.setFont("Helvetica-Bold", 14)  # Bold title
    c.drawString(200, height - 40, "Summary Report")

    # Total Revenue Section
    c.setFont("Helvetica", 12)
    c.drawString(40, height - 70, f"Total Revenue: ${total_revenue:,.2f}")
    y_position = height - 100  # Adjust starting position for the following sections

    # Total Revenue by Product Section
    c.setFont("Helvetica-Bold", 12)
    c.drawString(40, y_position, "Total Revenue by Product:")
    y_position -= 20  # Move down for list of products
    for _, row in revenue_by_product.iterrows():
        c.setFont("Helvetica", 10)  # Normal text
        c.drawString(30, y_position, f"{row['product_name']}: ${row['revenue']:.2f}")
        y_position -= 20
        
        # Check if the current y_position is too low and add a new page if necessary
        if y_position < 100:
            c.showPage()  # Create a new page
            y_position = height - 40  # Reset Y position for new page

    # Add the chart for Revenue by Product (Below the summary)
    if y_position < 200:  # If there's not enough space for the chart, add a new page
        c.showPage()
        y_position = height - 40
    c.drawString(40, y_position, "Revenue by Product:")
    c.drawImage("revenue_by_product.png", 40, y_position - 150, width=500, height=200)
    y_position -= 230  # Adjust position after the image

    # Total Revenue by Order Date Section
    c.setFont("Helvetica-Bold", 12)
    c.drawString(40, y_position, "Total Revenue by Order Date:")
    y_position -= 20  # Move down for the order date list
    for _, row in grouped.iterrows():
        c.setFont("Helvetica", 10)
        c.drawString(30, y_position, f"{row['order_date']}: ${row['revenue']:.2f}")
        y_position -= 20
        
        # Check if the current y_position is too low and add a new page if necessary
        if y_position < 100:
            c.showPage()
            y_position = height - 40

    # Add the chart for Total Revenue by Order Date (Below the summary)
    if y_position < 200:
        c.showPage()
        y_position = height - 40
    c.drawString(40, y_position, "Total Revenue By Order Date:")
    c.drawImage("total_revenue_by_date.png", 40, y_position - 150, width=500, height=200)
    y_position -= 180  # Adjust position after the image

    # Top-Selling Products Section (on a new page)
    c.showPage()  # Move the Top-Selling Products section to a new page
    c.setFont("Helvetica-Bold", 12)
    c.drawString(30, height - 40, "Top-Selling Products:")  # Title for the section
    y_position = height - 70  # Adjust position for the list

    # List the top-selling products
    for _, row in top_selling.iterrows():
        c.setFont("Helvetica", 10)
        c.drawString(30, y_position, f"{row['product_name']}: {row['quantity_sold']} units, ${row['revenue']:.2f}")
        y_position -= 20

        # Check if the current y_position is too low and add a new page if necessary
        if y_position < 100:
            c.showPage()
            y_position = height - 40  # Reset y_position for the new page

    # Add the chart for Top-Selling Products (Below the summary)
    if y_position < 200:
        c.showPage()  # Move to the next page if not enough space
        y_position = height - 40  # Reset Y position for the chart
    c.drawString(40, y_position, "Top-Selling Products:")
    c.drawImage("top_selling_products.png", 40, y_position - 180, width=500, height=200)
    y_position -= 230  # Adjust position after the image

    # Customer Segmentation Section
    c.setFont("Helvetica-Bold", 12)
    c.drawString(30, y_position, "Customer Segmentation:")
    y_position -= 20  # Move down for customer segmentation list
    for _, row in customer_segmentation.iterrows():
        c.setFont("Helvetica", 10)
        c.drawString(30, y_position, f"{row['segment']}: {row['customer_count']} customers")
        y_position -= 20
        
        # Check if the current y_position is too low and add a new page if necessary
        if y_position < 100:
            c.showPage()
            y_position = height - 40

    # Add the chart for Customer Segmentation (Below the summary)
    if y_position < 200:
        c.showPage()
        y_position = height - 40
    c.drawString(40, y_position, "Customer Segmentation:")
    c.drawImage("customer_segmentation_revenue_pie.png", 40, y_position - 180, width=600, height=200)
    y_position -= 230  # Adjust position after the image

    # Save the PDF
    c.save()
    print("✅ PDF report generated: summary_report_with_charts.pdf")


# ---------- Main Flow ----------
@flow
def ecommerce_pipeline(file_path: str):
    customers = read_csv("C:/Users/Bhumika/Desktop/DA Internship 2/Sprint 4" + '/customers.csv')
    orders = read_csv("C:/Users/Bhumika/Desktop/DA Internship 2/Sprint 4" + '/orders.csv')
    products = read_csv("C:/Users/Bhumika/Desktop/DA Internship 2/Sprint 4" + '/products.csv')
    sales = read_csv("C:/Users/Bhumika/Desktop/DA Internship 2/Sprint 4" + '/sales.csv')

    validate_data(customers, orders, products, sales)

    merged_data = merge_data(customers, orders, products)
    
    # Insert data into MySQL
    insert_to_mysql(customers, "customers")
    insert_to_mysql(orders, "orders")
    insert_to_mysql(products, "products")
    insert_to_mysql(sales, "sales")
    
    generate_reports(sales, products, customers, orders)

# Execute the flow with your CSV files path
ecommerce_pipeline("C:/Users/Bhumika/Desktop/DA Internship 2/Sprint 4")






