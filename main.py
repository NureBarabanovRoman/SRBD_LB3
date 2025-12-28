from fastapi import FastAPI, HTTPException, status
from typing import List, Dict, Any, Optional
import pyodbc 
from pydantic import BaseModel
from datetime import datetime

from database import fetch_all, fetch_one, execute_procedure, execute_function, execute_scalar_function

app = FastAPI(
    title="DB3 API",
    description="Lab3",
    version="1.0.0",
)

class Good(BaseModel):
    GOOD_ID: int
    NAME: str
    PRICE: float
    QUANTITY: int
    PRODUCER: Optional[str] = None
    DEPT_ID: int
    DESCRIPTION: Optional[str] = None

class Sale(BaseModel):
    SALE_ID: int
    GOOD_ID: int
    CHECK_NO: Optional[int] = None
    DATE_SALE: datetime
    QUANTITY: int

class GoodWithSales(Good):
    sales: List[Sale] = []

class MostSoldGood(BaseModel):
    GoodName: str
    TotalSold: Optional[int] = None

class SetDiscountRequest(BaseModel):
    good_name: str
    discount_percentage: int

class CreateGoodRequest(BaseModel):
    NAME: str
    PRICE: float
    QUANTITY: int
    PRODUCER: Optional[str] = None
    DEPT_ID: int
    DESCRIPTION: Optional[str] = None

class CreateSaleRequest(BaseModel):
    GOOD_ID: int
    CHECK_NO: Optional[int] = None
    QUANTITY: int

@app.get("/")
async def read_root():
    return {"message": "Go to /docs"}

@app.get("/goods", response_model=List[Good], summary="Get all goods")
async def get_all_goods():
    try:
        goods_data = await fetch_all("SELECT GOOD_ID, NAME, PRICE, QUANTITY, PRODUCER, DEPT_ID, DESCRIPTION FROM Goods")
        return goods_data
    except pyodbc.Error as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error: {e}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

@app.post("/goods", response_model=Good, summary="Create a new good")
async def create_good(good: CreateGoodRequest):
    if datetime.now().weekday() >= 5: # Saturday = 5, Sunday = 6
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="You cannot create goods at Saturday or Sunday")
    try:
        new_good_id = await execute_scalar_function(
            "INSERT INTO Goods (NAME, PRICE, QUANTITY, PRODUCER, DEPT_ID, DESCRIPTION) OUTPUT INSERTED.GOOD_ID VALUES (?, ?, ?, ?, ?, ?)",
            (good.NAME, good.PRICE, good.QUANTITY, good.PRODUCER, good.DEPT_ID, good.DESCRIPTION)
        )
        return Good(GOOD_ID=new_good_id, **good.dict())
    except pyodbc.Error as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error: {e}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

@app.post("/sales", response_model=Sale, summary="Create a new sale")
async def create_sale(sale: CreateSaleRequest):
    try:
        total_sales_result = await fetch_one(
            "SELECT SUM(QUANTITY) as total FROM Sales WHERE GOOD_ID = ?",
            (sale.GOOD_ID,)
        )
        total_sales = total_sales_result['total'] if total_sales_result and total_sales_result['total'] else 0

        if total_sales + sale.QUANTITY > 100:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot add sale: this product would exceed 100 sales")

        new_sale_id = await execute_scalar_function(
            "INSERT INTO Sales (GOOD_ID, CHECK_NO, DATE_SALE, QUANTITY) OUTPUT INSERTED.SALE_ID VALUES (?, ?, GETDATE(), ?)",
            (sale.GOOD_ID, sale.CHECK_NO, sale.QUANTITY)
        )

        await execute_procedure("INSERT INTO SalesLogs (SaleId, ModifyDate) VALUES (?, GETDATE())", (new_sale_id,))

        created_sale = await fetch_one("SELECT * FROM Sales WHERE SALE_ID = ?", (new_sale_id,))
        return created_sale

    except pyodbc.Error as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error: {e}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")


@app.get("/goods/{good_id}/sales", response_model=GoodWithSales, summary="Get details and sales for a specific good")
async def get_good_sales(good_id: int):
    try:
        good_data = await fetch_one(
            "SELECT GOOD_ID, NAME, PRICE, QUANTITY, PRODUCER, DEPT_ID, DESCRIPTION FROM Goods WHERE GOOD_ID = ?",
            (good_id,)
        )
        if not good_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Good not found")

        sales_data = await fetch_all(
            "SELECT SALE_ID, GOOD_ID, CHECK_NO, DATE_SALE, QUANTITY FROM Sales WHERE GOOD_ID = ?",
            (good_id,)
        )
        
        good_data["sales"] = [Sale(**sale) for sale in sales_data]
        
        return GoodWithSales(**good_data)
    except HTTPException as http_exc:
        raise http_exc
    except pyodbc.Error as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error: {e}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

@app.get("/workers/{worker_name}/most_sold_goods", response_model=List[MostSoldGood], summary="Get most sold goods by worker's department")
async def get_most_sold_goods_by_worker(worker_name: str):
    try:
        result = await execute_function("dbo.fn_MostSoldGoodsByWorker", (worker_name,))
        if not result:
            pass
        return result
    except pyodbc.Error as e:
        if "No sales" in str(e):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error: {e}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

@app.post("/goods/set_discount", summary="Set discount description for a good")
async def set_good_discount(request: SetDiscountRequest):
    try:
        await execute_procedure("dbo.sp_SetDiscountDescription", (request.good_name, request.discount_percentage))
        return {"message": f"Discount set for '{request.good_name}' to {request.discount_percentage}%"}
    except pyodbc.Error as e:
        error_message = str(e)
        if "Product not found" in error_message:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=error_message)
        elif "You cannot modify orders at Saturday or Sunday" in error_message:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)
        elif "Cannot add sale: this product would exceed 100 sales" in error_message:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error: {e}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

@app.get("/departments/{dept_id}/average_price", summary="Get average price by department")
async def get_average_price_by_department(dept_id: int):
    try:
        avg_price = await execute_scalar_function("SELECT dbo.AVG_PRICE_BY_DEPT(?)", (dept_id,))
        if avg_price is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Department not found or no goods in department")
        return {"department_id": dept_id, "average_price": avg_price}
    except pyodbc.Error as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error: {e}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")
