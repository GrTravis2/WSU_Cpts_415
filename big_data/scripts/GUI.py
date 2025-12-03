"""GUI for project."""

import os
import tkinter as tk
from tkinter import ttk

from PIL import Image, ImageTk
from scripts.analysis import analyze_links, graph_filter, trending_predictor


class AlgorithmGUI:
    """class for gui display of algorithms."""

    def __init__(self, root):
        """Create gui elements."""
        self.root = root
        self.root.title("Team TODO Milestone 4 GUI")
        self.current_photo = None

        # outlines
        self.button_frame = ttk.Frame(root, padding="10")
        self.image_frame = ttk.LabelFrame(root, text="Image Display", padding="10")
        self.text_frame = ttk.LabelFrame(root, text="Text Output", padding="10")
        self.button_frame.pack(side=tk.TOP, fill=tk.X)
        self.image_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.text_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.create_buttons()
        self.create_image_display()
        self.create_text_display()

    def create_buttons(self):
        """Create buttons for algorithms."""
        button_texts = ["Predict Trends", "Analyze Links", "Correlate Data", "Filter Graphs", "Reload Trends"]

        self.buttons = []

        for i, text in enumerate(button_texts):
            # make button and hook up to algo
            button = ttk.Button(self.button_frame, text=text, command=lambda idx=i: self.run_algorithm(idx + 1))
            # style
            button.pack(side=tk.LEFT, padx=5, pady=5)
            self.buttons.append(button)

    def create_image_display(self):
        """Create image display area."""
        # empty display on init
        self.image_label = ttk.Label(self.image_frame, text="No image loaded")
        self.image_label.pack(fill=tk.BOTH, expand=True)

    def create_text_display(self):
        """Create text display area."""
        # textbox w/ scrollbar
        self.textbox = tk.Text(self.text_frame, wrap=tk.WORD, width=40, height=20)
        scrollbar = ttk.Scrollbar(self.text_frame, orient=tk.VERTICAL, command=self.textbox.yview)
        self.textbox.configure(yscrollcommand=scrollbar.set)

        self.textbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def run_algorithm(self, algorithm_num):
        """Run selected algorithm and display results."""
        # clear prev text
        self.textbox.delete(1.0, tk.END)

        self.textbox.insert(tk.END, f"Running Algorithm {algorithm_num}...\n")
        self.textbox.insert(tk.END, f"Results from Algorithm {algorithm_num}:\n")

        match algorithm_num:
            case 1:  # trending predictor
                # read from txt file and display
                try:
                    self.load_algorithm_image(algorithm_num)
                    with open("text_outputs/trend_output.txt", "r") as file:
                        results = file.read()
                        self.textbox.insert(tk.END, results + "\n")
                except FileNotFoundError:
                    self.load_algorithm_image(algorithm_num)
                    trending_predictor.read_from_mongodb()
                    with open("text_outputs/trend_output.txt", "r") as file:
                        results = file.read()
                        self.textbox.insert(tk.END, results + "\n")

            case 2:  # analyze links
                self.textbox.insert(tk.END, "Analyze Links Results In Image\n")
                # load if already made, else run algo then load
                try:
                    self.load_algorithm_image(algorithm_num)
                except Exception as e:
                    self.textbox.insert(tk.END, f"Error loading image: {e}\n")
                    analyze_links.main()
                    analyze_links.main(show_results=True)
                    self.load_algorithm_image(algorithm_num)
            case 3:  # correlate data
                self.textbox.insert(tk.END, "Correlation Analysis TODO\n")
                self.load_algorithm_image(algorithm_num)
            case 4:  # filter graphs
                self.textbox.insert(tk.END, "Graph Filter Results In Image\n")
                # load if already made, else run algo then load
                try:
                    self.load_algorithm_image(algorithm_num)
                except Exception as e:
                    self.textbox.insert(tk.END, f"Error loading image: {e}\n")
                    graph_filter.main()
                    self.load_algorithm_image(algorithm_num)
            case 5:  # reload trends from mongodb and display
                self.load_algorithm_image(algorithm_num)
                trending_predictor.read_from_mongodb()
                with open("text_outputs/trend_output.txt", "r") as file:
                    results = file.read()
                    self.textbox.insert(tk.END, results + "\n")
            case _:
                self.load_algorithm_image(1)  # blank
                self.textbox.insert(tk.END, "Invalid algorithm number.\n")

    def load_algorithm_image(self, algorithm_num):
        """Load and display image for selected algorithm."""
        # images, 1 = trending predictor, 2 = analyze links, 3 = correlate data, 4 = filter graphs
        # output graphs to pictures folder and add paths here, one per algo
        image_files = {
            1: ["pictures/test1.jpg"],
            2: ["pictures/analyze_links.png"],
            3: ["pictures/correlation_heatmap.png"],
            4: ["pictures/graph_filter.png"],
        }

        image_candidates = image_files.get(algorithm_num, [])
        image_file = None

        # make sure file exists
        for candidate in image_candidates:
            if os.path.exists(candidate):
                image_file = candidate
                break

        if image_file:
            try:
                image = Image.open(image_file)
                display_size = (400, 300)
                image = image.resize(display_size, Image.Resampling.LANCZOS)

                photo = ImageTk.PhotoImage(image)
                self.image_label.configure(image=photo, text="")

                self.current_photo = photo

            except Exception as e:
                self.image_label.configure(image="", text=f"Error loading image:\n{str(e)}")
        else:
            # error for invalid images
            file_list = ", ".join(image_candidates)
            self.image_label.configure(image="", text=f"No image file found.\nTried: {file_list}")


def main() -> None:
    """Script entry point."""
    root = tk.Tk()
    app = AlgorithmGUI(root)  # noqa : F841
    # above is used due to init.
    root.geometry("800x600")
    root.mainloop()


if __name__ == "__main__":
    main()
