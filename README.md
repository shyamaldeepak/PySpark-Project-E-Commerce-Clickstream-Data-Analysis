# PySpark E-Commerce Clickstream Streaming Project

This project is a very simple real-time style PySpark project for beginners.

It pretends to be a small online shop.
People visit pages, add things to cart, and sometimes buy products.
Spark watches the new event files and calculates useful numbers.

## What You Will Learn

By building and running this project, you will learn:

- what a clickstream is
- how PySpark reads data in small pieces
- how real-time style streaming works
- how to count visits, users, and purchases
- how to calculate simple business metrics

## Very Simple Idea

Think of it like this:

1. We create fake customer activity.
2. We save that activity into JSON files.
3. Spark keeps checking the folder for new files.
4. Every few seconds, Spark counts things and shows the result.

So this is not a heavy production system.
It is a safe starter project for learning.

## Project Files

Here is what each main file does:

- `src/realtime_clickstream.py` - the Spark program that reads the data and calculates metrics
- `data/generate_clickstream_data.py` - creates fake clickstream data files
- `requirements.txt` - tells Python which package to install
- `data/` - stores generated sample data
- `output/` - stores Spark output and checkpoints

## What the Data Means

Each event in the data is one small action from a shopper.

Examples:

- `session_start` means a new shopping session began
- `page_view` means someone looked at a page
- `search` means someone searched for something
- `product_view` means someone looked at a product
- `cart_add` means someone added a product to the cart
- `checkout` means they moved toward buying
- `purchase` means they actually bought something

Each event also has details like:

- user id
- session id
- page name
- product id
- country
- device type
- price
- quantity

## Step 1: Install Python Dependencies

Before running anything, install PySpark and the other required Python packages.

Open a terminal inside this project folder and run:

```bash
pip install -r requirements.txt
```

If you are new, here is what that means:

- `pip` is the tool Python uses to install packages
- `install` means add a package to your computer
- `-r requirements.txt` means read the list of packages from that file and install all of them

If this step is skipped, the Spark script will fail with a missing package error.

If you run into a Java or Spark compatibility error, reinstall with the version pinned in `requirements.txt` and try again.

Important: this project needs a Java 17-compatible runtime for Spark. In this Codespaces workspace, Java 25 caused Spark to fail, so I used a local Java 17 install instead.

If your machine already has Java 17, you can skip the next note. If not, use a Java 17 JDK before running Spark.

In this workspace, the working Java path was:

```bash
$HOME/.local/jdk17/jdk-17.0.15+6
```

## Step 2: Create Sample Clickstream Data

Now we make fake shopping events.

Run this command:

```bash
python data/generate_clickstream_data.py --batches 5 --events-per-batch 50
```

What this means:

- `python` starts the Python program
- `data/generate_clickstream_data.py` is the file that creates data
- `--batches 5` means create 5 separate files
- `--events-per-batch 50` means each file has 50 events

After this runs, you will see JSON files appear in `data/landing`.

## Step 3: Start the Spark Streaming Job

Now Spark will watch the folder and read new files as they appear.

Run this command:

```bash
python src/realtime_clickstream.py --input data/landing --sink console
```

If your computer needs the same Java 17 override used in this workspace, run:

```bash
JAVA_HOME=$HOME/.local/jdk17/jdk-17.0.15+6 PATH=$HOME/.local/jdk17/jdk-17.0.15+6/bin:$PATH python src/realtime_clickstream.py --input data/landing --sink console
```

What this means:

- `src/realtime_clickstream.py` is the main Spark program
- `--input data/landing` tells Spark where to look for files
- `--sink console` tells Spark to print results on the screen

If everything is working, Spark will keep running and show metrics every few seconds.

## Step 4: Run It the Easy Way

If you want the simplest beginner flow, use two terminal windows.

Terminal 1:

```bash
pip install -r requirements.txt
python data/generate_clickstream_data.py --batches 5 --events-per-batch 50
```

Terminal 2:

```bash
python src/realtime_clickstream.py --input data/landing --sink console
```

What happens next:

1. The first terminal creates the input files.
2. The second terminal reads those files.
3. Spark prints live results on the screen.

If you already created the data folder once, you only need to run the generator again when you want fresh events.

## Step 5: Watch the Output

When Spark sees the data, it calculates numbers like:

- how many events happened
- how many unique users were active
- how much revenue was made from purchases
- how many people added items to cart
- how many sessions turned into purchases
- which pages were viewed most often

If you are new, do not worry about every Spark term yet.
Just remember this:

- input files go in
- Spark reads them
- Spark counts useful things
- output comes out

## Step 6: Save Results to Files Instead of Console

If you want Spark to write results into files, use parquet mode:

```bash
python src/realtime_clickstream.py --input data/landing --sink parquet --output output/metrics
```

This is useful if you want to:

- check results later
- build a dashboard
- use the data in another program

## Step 7: Understand the Metrics

Here is what each metric means in simple words:

- events per minute: how busy the shop is
- unique active users per minute: how many different people were active
- total revenue: how much money purchases made
- cart-to-purchase rate: how often people who add items also buy them
- top pages: which pages people visit most
- session conversion rate: how many sessions ended in a purchase

These are useful because they tell you if the shop is healthy.

## Step 8: What the Generator Is Doing

The generator is just a helper script.

It does this:

1. Makes a fake event
2. Gives it a time, user, session, and event type
3. Writes it into a JSON file
4. Repeats until the batch is full
5. Makes more files if you asked for more batches

This is why the project feels like real-time data.
New files keep arriving, and Spark keeps reading them.

## Step 9: Try It Again and Again

You can run the generator more than once.

If you want more data, use:

```bash
python data/generate_clickstream_data.py --batches 10 --events-per-batch 100
```

If you want smaller data for practice, use:

```bash
python data/generate_clickstream_data.py --batches 2 --events-per-batch 10
```

## Step 10: Where the Output Files Go

Spark may create these folders:

- `data/landing` for input files
- `output/metrics` for saved metric files
- `output/checkpoints` for Spark checkpoint data

Checkpoint files help Spark remember where it left off.
That way, it does not read the same file again and again.

## Step 11: How to Clean Everything Up

If you want a fresh start, delete the generated folders:

- `data/landing`
- `output/`

Then run the generator again.

## Troubleshooting

If Spark fails to start and you see a message about `getSubject is not supported`, make sure you installed the pinned dependency with:

```bash
pip install -r requirements.txt
```

Then run the project again from a fresh terminal.

## Beginner Practice Ideas

Once the basic version works, try these small improvements:

1. Add a new event type like `wishlist_add`
2. Show results by country
3. Show results by device type
4. Save the output in a format that a dashboard can read easily
5. Detect traffic spikes

## Short Summary

This project teaches real-time data basics using a simple folder of JSON files.

The full loop is:

1. generate data
2. start Spark
3. watch metrics update
4. explore the results

If you can run this project once, you already understand the core idea of a streaming pipeline.
