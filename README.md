# Spark DataFrame un-pivoting and splitting

This is an example project demonstrating how a DataFrame can be 
un-pivoted/melted`and then how the column values themselves can be split
into attributes of the data.

This is a common scenario where existing data sets are structured to allow
end users to manually view or manipulate data. They are often structured
so that the format is easily readable by people.

If we had an example input file which looked as follows:

StoreId | Frozen - Peas | Frozen - Sweetcorn | Frozen - Chicken | FruitVeg - Cucumbers
------- | ------------- | ------------------ | ---------------- | --------------------
1       | 13            | 14                 | 15               | 16
2       | 17            | 18                 | 19               | 20

In this scenario we want to turn it into the following DataFrame:

StoreId | Category | Type      | ItemsSold
------- | -------- | --------- | ---------
1       | Frozen   | Peas      | 13
1       | Frozen   | Sweetcorn | 14
1       | Frozen   | Chicken   | 15
1       | FruitVeg | Cucumbers | 16
2       | Frozen   | Peas      | 17
2       | Frozen   | Sweetcorn | 18
2       | Frozen   | Chicken   | 19
2       | FruitVeg | Cucumbers | 20

In this scenario we want to be able to transform data so that we can report
on it (e.g. summary of items sold), and be able to join to other data where
information is held with a category and type attribute.
