import tkinter as tk
from tkinter import ttk, messagebox
from lstore.db import Database
from lstore.query import Query

class DatabaseGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("L-Store Database GUI")
        self.root.geometry("600x400")

        self.db = Database()
        self.db.open("./ECS165")

        self.create_widgets()

    def create_widgets(self):
        style = ttk.Style()
        style.configure("TFrame", background="#f0f0f0")
        style.configure("TLabel", background="#f0f0f0", font=("Arial", 12))
        style.configure("TButton", font=("Arial", 12))

        # Create Table Section
        self.create_table_frame = ttk.Frame(self.root, padding="10")
        self.create_table_frame.pack(pady=10)

        self.table_name_label = ttk.Label(self.create_table_frame, text="Table Name:")
        self.table_name_label.grid(row=0, column=0, padx=5, pady=5)
        self.table_name_entry = ttk.Entry(self.create_table_frame)
        self.table_name_entry.grid(row=0, column=1, padx=5, pady=5)

        self.num_columns_label = ttk.Label(self.create_table_frame, text="Number of Columns:")
        self.num_columns_label.grid(row=1, column=0, padx=5, pady=5)
        self.num_columns_entry = ttk.Entry(self.create_table_frame)
        self.num_columns_entry.grid(row=1, column=1, padx=5, pady=5)

        self.key_index_label = ttk.Label(self.create_table_frame, text="Key Index:")
        self.key_index_label.grid(row=2, column=0, padx=5, pady=5)
        self.key_index_entry = ttk.Entry(self.create_table_frame)
        self.key_index_entry.grid(row=2, column=1, padx=5, pady=5)

        self.create_table_button = ttk.Button(self.create_table_frame, text="Create Table", command=self.create_table)
        self.create_table_button.grid(row=3, columnspan=2, pady=10)

        # Insert Record Section
        self.insert_frame = ttk.Frame(self.root, padding="10")
        self.insert_frame.pack(pady=10)

        self.insert_label = ttk.Label(self.insert_frame, text="Insert Record (comma-separated values):")
        self.insert_label.grid(row=0, column=0, padx=5, pady=5)
        self.insert_entry = ttk.Entry(self.insert_frame, width=50)
        self.insert_entry.grid(row=0, column=1, padx=5, pady=5)

        self.insert_button = ttk.Button(self.insert_frame, text="Insert", command=self.insert_record)
        self.insert_button.grid(row=1, columnspan=2, pady=10)

        # Select Record Section
        self.select_frame = ttk.Frame(self.root, padding="10")
        self.select_frame.pack(pady=10)

        self.select_label = ttk.Label(self.select_frame, text="Select Record by Key:")
        self.select_label.grid(row=0, column=0, padx=5, pady=5)
        self.select_entry = ttk.Entry(self.select_frame)
        self.select_entry.grid(row=0, column=1, padx=5, pady=5)

        self.select_button = ttk.Button(self.select_frame, text="Select", command=self.select_record)
        self.select_button.grid(row=1, columnspan=2, pady=10)

        # Update Record Section
        self.update_frame = ttk.Frame(self.root, padding="10")
        self.update_frame.pack(pady=10)

        self.update_label = ttk.Label(self.update_frame, text="Update Record by Key:")
        self.update_label.grid(row=0, column=0, padx=5, pady=5)
        self.update_key_entry = ttk.Entry(self.update_frame)
        self.update_key_entry.grid(row=0, column=1, padx=5, pady=5)

        self.update_values_label = ttk.Label(self.update_frame, text="New Values (comma-separated):")
        self.update_values_label.grid(row=1, column=0, padx=5, pady=5)
        self.update_values_entry = ttk.Entry(self.update_frame, width=50)
        self.update_values_entry.grid(row=1, column=1, padx=5, pady=5)

        self.update_button = ttk.Button(self.update_frame, text="Update", command=self.update_record)
        self.update_button.grid(row=2, columnspan=2, pady=10)

        # Output Section
        self.output_frame = ttk.Frame(self.root, padding="10")
        self.output_frame.pack(pady=10)

        self.output_text = tk.Text(self.output_frame, height=10, width=80, font=("Arial", 12))
        self.output_text.pack()

        # Animation
        self.animate_button(self.create_table_button)
        self.animate_button(self.insert_button)
        self.animate_button(self.select_button)
        self.animate_button(self.update_button)

    def animate_button(self, button):
        def on_enter(event):
            button.config(style="Hover.TButton")

        def on_leave(event):
            button.config(style="TButton")

        button.bind("<Enter>", on_enter)
        button.bind("<Leave>", on_leave)

        style = ttk.Style()
        style.configure("TButton", font=("Arial", 12), background="#2196F3", foreground="#00FF00")
        style.configure("Hover.TButton", font=("Arial", 12, "bold"), background="#1976D2", foreground="#00FF00")

    def create_table(self):
        table_name = self.table_name_entry.get()
        num_columns = int(self.num_columns_entry.get())
        key_index = int(self.key_index_entry.get())

        try:
            self.db.create_table(table_name, num_columns, key_index)
            messagebox.showinfo("Success", f"Table '{table_name}' created successfully.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def insert_record(self):
        table_name = self.table_name_entry.get()
        table = self.db.get_table(table_name)
        query = Query(table)

        record_values = self.insert_entry.get().split(',')
        record_values = [int(value.strip()) for value in record_values]

        try:
            if query.insert(*record_values):
                messagebox.showinfo("Success", "Record inserted successfully.")
            else:
                messagebox.showerror("Error", "Failed to insert record.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def select_record(self):
        table_name = self.table_name_entry.get()
        table = self.db.get_table(table_name)
        query = Query(table)

        key = int(self.select_entry.get())

        try:
            records = query.select(key, table.key, [1] * table.num_columns)
            self.output_text.delete(1.0, tk.END)
            for record in records:
                self.output_text.insert(tk.END, str(record) + "\n")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def update_record(self):
        table_name = self.table_name_entry.get()
        table = self.db.get_table(table_name)
        query = Query(table)

        key = int(self.update_key_entry.get())
        new_values = self.update_values_entry.get().split(',')
        new_values = [int(value.strip()) for value in new_values]
        for i in range(len(new_values)):
            if (new_values[i] == -1):
                new_values[i] = None

        try:
            if query.update(key, *new_values):
                messagebox.showinfo("Success", "Record updated successfully.")
            else:
                messagebox.showerror("Error", "Failed to update record.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

if __name__ == "__main__":
    root = tk.Tk()
    app = DatabaseGUI(root)
    root.mainloop()
    app.db.close()

    