# Databricks notebook source
# DBTITLE 1,User Guide - How to Use These Study Materials
# MAGIC %md
# MAGIC # User Guide: How to Use These Study Materials
# MAGIC
# MAGIC This guide explains how to use your Data Engineer Associate certification prep materials and how to work with AI assistance during study sessions.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Quick Start
# MAGIC %md
# MAGIC ## Quick Start
# MAGIC
# MAGIC ### Your Study Materials Structure
# MAGIC
# MAGIC ```
# MAGIC databricks_certification_prep/
# MAGIC ├── REPLICATION_GUIDE.py       ← Meta-guide for replicating structure
# MAGIC ├── Databricks exam guides/
# MAGIC └── Data Engineer Associate/
# MAGIC     ├── 00_getting_started.py  ← You are here
# MAGIC     ├── 01_study_plan.py       ← Complete roadmap and schedule
# MAGIC     ├── 01_Databricks_Lakehouse_Platform/
# MAGIC     │   ├── overview.py            # Reference material
# MAGIC     │   ├── practice_tasks.py      # Exercises
# MAGIC     │   └── solutions.py           # Solutions and validation
# MAGIC     ├── 02_ELT_Spark_SQL_Python/
# MAGIC     │   ├── overview.py
# MAGIC     │   ├── practice_tasks.py
# MAGIC     │   └── solutions.py
# MAGIC     ... (9 topics total)
# MAGIC ```
# MAGIC
# MAGIC ### The 3-Notebook System
# MAGIC
# MAGIC Each topic has **three notebooks** that work together:
# MAGIC
# MAGIC 1. **overview.py**
# MAGIC    - Comprehensive reference material
# MAGIC    - Code examples and explanations
# MAGIC    - Configuration tables
# MAGIC    - Best practices
# MAGIC    - **When to use**: Read first to learn concepts
# MAGIC
# MAGIC 2. **practice_tasks.py**
# MAGIC    - 15 exercises (easy → medium → hard)
# MAGIC    - 5 multiple choice questions
# MAGIC    - 2 challenge scenarios
# MAGIC    - 2 ETL applied tasks
# MAGIC    - **When to use**: After reading overview, practice here
# MAGIC
# MAGIC 3. **solutions.py**
# MAGIC    - Complete working solutions
# MAGIC    - Detailed explanations
# MAGIC    - Alternative approaches
# MAGIC    - Validation functions
# MAGIC    - MCQ answers with explanations
# MAGIC    - **When to use**: After attempting exercises, check your work
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,The Complete Workflow
# MAGIC %md
# MAGIC ## The Complete Workflow
# MAGIC
# MAGIC ### Step-by-Step Process
# MAGIC
# MAGIC #### Phase 1: Read & Understand
# MAGIC **Open**: `overview.py` for the topic
# MAGIC
# MAGIC 1. Read through all concepts sequentially
# MAGIC 2. Run the code examples to see them in action
# MAGIC 3. Pay attention to configuration tables
# MAGIC 4. Bookmark important sections
# MAGIC
# MAGIC **Note**: Type out examples yourself in scratch cells rather than just reading. This builds muscle memory for exam syntax.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Phase 2: Practice
# MAGIC **Open**: `practice_tasks.py` for the topic
# MAGIC
# MAGIC 1. **Run the setup cell** at the top (creates sample data)
# MAGIC 2. **Read Exercise 1** carefully
# MAGIC 3. **Write your solution** in the empty cell provided
# MAGIC 4. **Test your code** - run the cell
# MAGIC 5. **Repeat** for each exercise
# MAGIC
# MAGIC **Note**: Attempt exercises without consulting solutions first. Productive struggle improves retention.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Phase 3: Get Help (This is where AI comes in!)
# MAGIC **When you're stuck or want to check your work**:
# MAGIC
# MAGIC 1. **Ask me to check your solution**:
# MAGIC    ```
# MAGIC    "Can you check my solution for Exercise 5?"
# MAGIC    "Is my MERGE statement correct?"
# MAGIC    "Why isn't my code working?"
# MAGIC    ```
# MAGIC
# MAGIC 2. **Ask me to compare with the reference**:
# MAGIC    ```
# MAGIC    "How does my solution compare to the reference solution?"
# MAGIC    "Is there a more efficient way to do this?"
# MAGIC    ```
# MAGIC
# MAGIC 3. **Ask me to run validation**:
# MAGIC    ```
# MAGIC    "Run the validation function for Exercise 5"
# MAGIC    "Does my output match the expected result?"
# MAGIC    ```
# MAGIC
# MAGIC 4. **Ask for hints without spoilers**:
# MAGIC    ```
# MAGIC    "Give me a hint for Exercise 8 without showing the solution"
# MAGIC    "What concept should I use here?"
# MAGIC    ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Phase 4: Review Solutions
# MAGIC **Open**: `solutions.py` for the topic
# MAGIC
# MAGIC 1. **Find your exercise** in the solutions notebook
# MAGIC 2. **Compare** your approach with the reference solution
# MAGIC 3. **Read the explanation** to understand why it works
# MAGIC 4. **Check "Common Mistakes"** - did you make any?
# MAGIC 5. **Read the "Exam Tip"** - this is high-value info!
# MAGIC 6. **Try alternative solutions** if provided
# MAGIC
# MAGIC **Note**: Read explanations even for correct solutions. Alternative approaches and optimization patterns are documented there.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Phase 5: Self-Assessment
# MAGIC **Before moving to the next topic, check**:
# MAGIC
# MAGIC - [ ] Can you explain each concept from the overview?
# MAGIC - [ ] Did you complete all 15 exercises?
# MAGIC - [ ] Did you answer all 5 MCQs correctly?
# MAGIC - [ ] Did you attempt both challenge scenarios?
# MAGIC - [ ] Did you complete both ETL tasks?
# MAGIC - [ ] Can you write the key syntax from memory?
# MAGIC - [ ] Do you understand the exam tips?
# MAGIC
# MAGIC **If all boxes checked**: Proceed to next topic.
# MAGIC
# MAGIC **If not**: Revisit weak areas, repeat exercises, or request clarification.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Working with AI Assistance
# MAGIC %md
# MAGIC ## Working with AI Assistance
# MAGIC
# MAGIC ### Available Assistance
# MAGIC
# MAGIC #### 1. Check Your Solutions
# MAGIC
# MAGIC **What to ask**:
# MAGIC - "Check my solution for Exercise 3"
# MAGIC - "Is my code correct?"
# MAGIC - "Does my output match the expected result?"
# MAGIC
# MAGIC **What I'll do**:
# MAGIC - Read your code from the practice_tasks notebook
# MAGIC - Load the validation function from solutions notebook
# MAGIC - Run it against your output
# MAGIC - Tell you if it's correct or explain what's wrong
# MAGIC
# MAGIC **Example**:
# MAGIC ```
# MAGIC You: "Check my solution for Exercise 5 in Topic 3"
# MAGIC
# MAGIC Me: I'll:
# MAGIC 1. Open practice_tasks.py for Topic 3
# MAGIC 2. Read your code from Exercise 5 cell
# MAGIC 3. Open solutions.py for Topic 3
# MAGIC 4. Load the validation function
# MAGIC 5. Run validation and report results
# MAGIC 6. If wrong, explain the issue
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2. Explain Why Something's Wrong
# MAGIC
# MAGIC **What to ask**:
# MAGIC - "Why is my MERGE statement failing?"
# MAGIC - "I'm getting a schema mismatch error - help!"
# MAGIC - "What's wrong with this syntax?"
# MAGIC
# MAGIC **What I'll do**:
# MAGIC - Analyze your code
# MAGIC - Identify the issue
# MAGIC - Explain what's wrong and why
# MAGIC - Show you the correct approach
# MAGIC - Connect it back to concepts from the overview
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3. Compare Your Approach
# MAGIC
# MAGIC **What to ask**:
# MAGIC - "How does my solution compare to the reference?"
# MAGIC - "Is my approach better or worse?"
# MAGIC - "What's a more efficient way to do this?"
# MAGIC
# MAGIC **What I'll do**:
# MAGIC - Read both your solution and the reference
# MAGIC - Compare approaches
# MAGIC - Explain trade-offs
# MAGIC - Suggest improvements
# MAGIC - Point out exam-relevant considerations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 4. Provide Strategic Hints
# MAGIC
# MAGIC **What to ask**:
# MAGIC - "Give me a hint for Exercise 10 without spoiling it"
# MAGIC - "What concept should I use here?"
# MAGIC - "Am I on the right track?"
# MAGIC
# MAGIC **What I'll do**:
# MAGIC - Give you directional guidance
# MAGIC - Suggest relevant concepts to review
# MAGIC - Ask leading questions
# MAGIC - NOT give you the answer directly
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 5. Run Validation Functions
# MAGIC
# MAGIC **What to ask**:
# MAGIC - "Run the validation for Exercise 7"
# MAGIC - "Validate my DataFrame against expected output"
# MAGIC - "Check if my MCQ answers are correct"
# MAGIC
# MAGIC **What I'll do**:
# MAGIC - Load the appropriate validation function
# MAGIC - Run it with your results
# MAGIC - Show you detailed feedback
# MAGIC - Identify specific differences if wrong
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 6. Explain Concepts
# MAGIC
# MAGIC **What to ask**:
# MAGIC - "I don't understand Z-ORDER - explain it again"
# MAGIC - "What's the difference between OPTIMIZE and VACUUM?"
# MAGIC - "Why do we need checkpointing in streaming?"
# MAGIC
# MAGIC **What I'll do**:
# MAGIC - Re-explain the concept in different words
# MAGIC - Provide additional examples
# MAGIC - Connect to exam context
# MAGIC - Suggest which overview section to re-read
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 7. Prepare You for Exam Questions
# MAGIC
# MAGIC **What to ask**:
# MAGIC - "Quiz me on Delta Lake operations"
# MAGIC - "Give me a practice question on MERGE syntax"
# MAGIC - "Test my understanding of Spark configs"
# MAGIC
# MAGIC **What I'll do**:
# MAGIC - Create exam-style questions
# MAGIC - Check your answers
# MAGIC - Explain correct answers
# MAGIC - Point out common traps
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Understanding Validation Functions
# MAGIC %md
# MAGIC ## Understanding Validation Functions
# MAGIC
# MAGIC ### What Are They?
# MAGIC
# MAGIC Validation functions are Python functions in the solutions notebooks that **automatically check if your answer is correct**.
# MAGIC
# MAGIC ### How Do They Work?
# MAGIC
# MAGIC **Example from solutions.py**:
# MAGIC ```python
# MAGIC def validate_exercise_3(student_df, expected_df):
# MAGIC     """
# MAGIC     Checks if your DataFrame matches the expected output.
# MAGIC     """
# MAGIC     # Check schema
# MAGIC     if student_df.schema != expected_df.schema:
# MAGIC         print("❌ Schema mismatch")
# MAGIC         print(f"Your schema: {student_df.schema}")
# MAGIC         print(f"Expected: {expected_df.schema}")
# MAGIC         return False
# MAGIC     
# MAGIC     # Check row count
# MAGIC     if student_df.count() != expected_df.count():
# MAGIC         print(f"❌ Row count: got {student_df.count()}, expected {expected_df.count()}")
# MAGIC         return False
# MAGIC     
# MAGIC     # Check data
# MAGIC     diff = student_df.exceptAll(expected_df).count()
# MAGIC     if diff > 0:
# MAGIC         print(f"❌ {diff} rows don't match")
# MAGIC         return False
# MAGIC     
# MAGIC     print("✅ Correct! Your solution matches the expected output.")
# MAGIC     return True
# MAGIC ```
# MAGIC
# MAGIC ### How to Use Them
# MAGIC
# MAGIC #### Option 1: Ask Me to Run It
# MAGIC ```
# MAGIC You: "Run validation for Exercise 3"
# MAGIC
# MAGIC Me: I'll open both notebooks, grab your result, 
# MAGIC     run the validation, and tell you the outcome.
# MAGIC ```
# MAGIC
# MAGIC #### Option 2: Run It Yourself
# MAGIC 1. **In practice_tasks**: Create your result DataFrame
# MAGIC 2. **In solutions**: Find the validation function
# MAGIC 3. **Copy** the validation function to a new cell in practice_tasks
# MAGIC 4. **Call it**: `validate_exercise_3(my_result, expected_result)`
# MAGIC 5. **Read the feedback**
# MAGIC
# MAGIC ### What Validation Checks
# MAGIC
# MAGIC - **Schema**: Column names and data types
# MAGIC - **Row Count**: Expected number of rows
# MAGIC - **Data Accuracy**: Exact data matching
# MAGIC - **Edge Cases**: Null handling, duplicate removal, etc.
# MAGIC
# MAGIC ### When Validation Fails
# MAGIC
# MAGIC The function will tell you **specifically what's wrong**:
# MAGIC
# MAGIC ```
# MAGIC ❌ Schema mismatch
# MAGIC Your schema: StructType([StructField('id', IntegerType(), True), ...])
# MAGIC Expected: StructType([StructField('customer_id', IntegerType(), True), ...])
# MAGIC ```
# MAGIC
# MAGIC This tells you: "You named it 'id' but it should be 'customer_id'"
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Study Tips & Best Practices
# MAGIC %md
# MAGIC ## Study Tips and Best Practices
# MAGIC
# MAGIC ### For Maximum Learning
# MAGIC
# MAGIC #### 1. Active Reading
# MAGIC **Ineffective approach**: Just read the overview and move on
# MAGIC
# MAGIC **Effective approach**: 
# MAGIC - Type out code examples yourself
# MAGIC - Modify examples to test your understanding
# MAGIC - Create your own examples
# MAGIC - Explain concepts out loud
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2. Struggle Before Solutions
# MAGIC **Ineffective approach**: Look at solutions immediately when stuck
# MAGIC
# MAGIC **Effective approach**:
# MAGIC - Spend 15-20 minutes trying different approaches
# MAGIC - Review the relevant overview section
# MAGIC - Ask me for a hint (not the full solution)
# MAGIC - THEN check the solution
# MAGIC
# MAGIC **Why**: Research shows struggling before seeing the answer leads to deeper learning.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3. Spaced Repetition
# MAGIC **Ineffective approach**: Cram all 9 topics in one weekend
# MAGIC
# MAGIC **Effective approach**:
# MAGIC - Follow the 6-week Master Plan schedule
# MAGIC - Review previous topics weekly
# MAGIC - Redo exercises you got wrong
# MAGIC - Quiz yourself on key concepts
# MAGIC
# MAGIC **Why**: Spaced practice = better long-term retention
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 4. Focus on Weak Areas
# MAGIC **Ineffective approach**: Spend equal time on everything
# MAGIC
# MAGIC **Effective approach**:
# MAGIC - Spend 70% of time on your weak areas:
# MAGIC   - PySpark DataFrame API (you're less familiar)
# MAGIC   - Streaming concepts (limited experience)
# MAGIC   - Spark configurations (exam-heavy)
# MAGIC - Quick review strong areas (SQL, Python basics)
# MAGIC
# MAGIC **Why**: The exam will test your weaknesses more than your strengths
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 5. Memorize Key Syntax
# MAGIC **Ineffective approach**: Assume you can figure it out during the exam
# MAGIC
# MAGIC **Effective approach**: Memorize these patterns cold:
# MAGIC - MERGE INTO syntax
# MAGIC - OPTIMIZE with Z-ORDER
# MAGIC - VACUUM commands
# MAGIC - Auto Loader patterns
# MAGIC - Key Spark configurations
# MAGIC - Pipeline expectations
# MAGIC
# MAGIC **How**: Create flashcards, write from memory daily
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 6. Simulate Exam Conditions
# MAGIC **Ineffective approach**: Only practice with notebooks and AI help
# MAGIC
# MAGIC **Effective approach**:
# MAGIC - Time yourself on MCQs (2 min per question)
# MAGIC - Write code without autocomplete
# MAGIC - Recall syntax without looking
# MAGIC - Answer questions without docs
# MAGIC
# MAGIC **Why**: The exam has no AI, no docs, limited time
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 7. Understand WHY, Not Just HOW
# MAGIC **Ineffective approach**: Just memorize commands
# MAGIC
# MAGIC **Effective approach**: Understand:
# MAGIC - Why VACUUM has a default retention period
# MAGIC - Why OPTIMIZE reduces small files
# MAGIC - Why streaming needs checkpoints
# MAGIC - Why certain configs matter
# MAGIC
# MAGIC **Why**: Exam has conceptual questions, not just syntax
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Study Schedule Recommendations
# MAGIC
# MAGIC #### Daily (Weekdays)
# MAGIC - **60-90 minutes** focused study
# MAGIC - Complete 3-5 exercises
# MAGIC - Review 1-2 overview concepts
# MAGIC - Quiz yourself on previous material
# MAGIC
# MAGIC #### Weekly (Weekends)
# MAGIC - **2-3 hours** each day
# MAGIC - Complete a full topic
# MAGIC - Review all previous topics
# MAGIC - Take practice MCQs
# MAGIC - Attempt challenge scenarios
# MAGIC
# MAGIC #### The Week Before Exam
# MAGIC - **Focus**: Configuration tables, syntax patterns
# MAGIC - **Practice**: Timed MCQs, write code from memory
# MAGIC - **Review**: Common traps, exam tips
# MAGIC - **Rest**: Get good sleep!
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Troubleshooting Common Issues
# MAGIC %md
# MAGIC ## Troubleshooting Common Issues
# MAGIC
# MAGIC ### Issue 1: Setup Code Fails
# MAGIC
# MAGIC **Symptoms**: The setup cell in practice_tasks won't run
# MAGIC
# MAGIC **Solutions**:
# MAGIC 1. Make sure you're running cells in order (top to bottom)
# MAGIC 2. Check if you have compute attached (serverless will auto-attach)
# MAGIC 3. Restart the notebook and try again
# MAGIC 4. Ask me: "The setup code in Topic X is failing - help!"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 2: My Code Works But Validation Fails
# MAGIC
# MAGIC **Symptoms**: Your code produces output, but validation says it's wrong
# MAGIC
# MAGIC **Solutions**:
# MAGIC 1. Check the **schema** - are column names exactly right?
# MAGIC 2. Check **data types** - is `id` an Integer or String?
# MAGIC 3. Check **row count** - did you filter too much or too little?
# MAGIC 4. Check **null handling** - are nulls where they should be?
# MAGIC 5. Ask me: "My code works but validation fails - what's different?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 3: I Don't Understand the Exercise
# MAGIC
# MAGIC **Symptoms**: The exercise instructions are unclear
# MAGIC
# MAGIC **Solutions**:
# MAGIC 1. Re-read the "Scenario" and "Objective"
# MAGIC 2. Check the "Expected Output" description
# MAGIC 3. Review the relevant concept in the overview
# MAGIC 4. Ask me: "Can you explain Exercise X in simpler terms?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 4: Exercise Too Easy/Hard
# MAGIC
# MAGIC **Symptoms**: Exercises don't match your skill level
# MAGIC
# MAGIC **Solutions**:
# MAGIC
# MAGIC **If too easy**:
# MAGIC - Skip to medium/hard exercises (6-15)
# MAGIC - Jump straight to challenge scenarios
# MAGIC - Focus on ETL applied tasks
# MAGIC - Ask me: "Give me a harder version of this exercise"
# MAGIC
# MAGIC **If too hard**:
# MAGIC - Go back to the overview for that concept
# MAGIC - Try easier exercises first (1-5)
# MAGIC - Ask me for hints
# MAGIC - Review worked examples
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 5: Can't Remember Syntax
# MAGIC
# MAGIC **Symptoms**: You know the concept but forget exact syntax
# MAGIC
# MAGIC **Solutions**:
# MAGIC 1. Keep a "syntax cheat sheet" notebook
# MAGIC 2. Write flashcards for key patterns
# MAGIC 3. Practice writing from memory daily
# MAGIC 4. Use the configuration tables in overviews as reference
# MAGIC 5. Ask me: "What's the syntax for X again?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 6: Running Out of Time
# MAGIC
# MAGIC **Symptoms**: 6-week plan seems too ambitious
# MAGIC
# MAGIC **Solutions**:
# MAGIC 1. **Prioritize**: Focus on Topics 3 & 7 (highest exam weight)
# MAGIC 2. **Skip easy exercises**: If you already know it, move on
# MAGIC 3. **Reduce practice**: Do 10 exercises instead of 15
# MAGIC 4. **Extend timeline**: 8-10 weeks is still good!
# MAGIC 5. Ask me: "Help me create a condensed study plan"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 7: Lost Track of Progress
# MAGIC
# MAGIC **Symptoms**: Not sure what you've completed
# MAGIC
# MAGIC **Solutions**:
# MAGIC 1. Use the **Progress Tracker** in the Master Plan
# MAGIC 2. Check off exercises as you complete them
# MAGIC 3. Keep notes in the "Weak Areas Log"
# MAGIC 4. Ask me: "Show me what I've completed so far" (I can check which cells have been run)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 8: Need More Practice
# MAGIC
# MAGIC **Symptoms**: You've completed all exercises but want more
# MAGIC
# MAGIC **Solutions**:
# MAGIC 1. **Redo exercises** from memory without looking at solutions
# MAGIC 2. **Modify exercises**: Change requirements, use different data
# MAGIC 3. **Create your own**: Combine concepts from multiple topics
# MAGIC 4. **Take official practice exam**: Databricks provides these
# MAGIC 5. Ask me: "Create a custom exercise on [topic]"
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Example AI Conversation
# MAGIC %md
# MAGIC ## Example: AI-Assisted Study Session
# MAGIC
# MAGIC Typical study session with AI assistance:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **You**: I'm starting Topic 3 - Delta Lake Operations
# MAGIC
# MAGIC **AI**: Topic 3 is high-priority for the exam. Recommended approach:
# MAGIC 1. Start with the overview.py - focus on MERGE, OPTIMIZE, and VACUUM syntax
# MAGIC 2. Pay special attention to the configuration table
# MAGIC 3. When you get to practice tasks, MERGE exercises are critical - take your time
# MAGIC 4. Let me know when you're ready to start exercises!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **You**: [30 minutes later] Okay, I've read the overview. Starting exercises now.
# MAGIC
# MAGIC **AI**: Run the setup cell first. Start with Exercise 1 and work sequentially. Request assistance if needed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **You**: [Working through exercises] I'm stuck on Exercise 5 - the MERGE statement isn't working
# MAGIC
# MAGIC **AI**: Let me take a look... [reads your code]
# MAGIC
# MAGIC I see the issue! In your MERGE statement, you're missing the `WHEN NOT MATCHED` clause. Right now you're only updating existing rows, but the exercise asks you to also insert new rows.
# MAGIC
# MAGIC Try adding:
# MAGIC ```sql
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC Does that make sense?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **You**: Yes! That fixed it. Can you check if my solution is correct now?
# MAGIC
# MAGIC **AI**: [runs validation function]
# MAGIC
# MAGIC Your solution is correct. DataFrame matches expected output.
# MAGIC
# MAGIC Correct elements:
# MAGIC - ON clause matches on customer_id
# MAGIC - UPDATE SET syntax is proper
# MAGIC - INSERT clause handles new records
# MAGIC
# MAGIC Proceed to Exercise 6.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **You**: [Later] I finished all 15 exercises! Can you check my MCQ answers?
# MAGIC
# MAGIC **AI**: [checks your answers against solutions]
# MAGIC
# MAGIC You got 4 out of 5 correct! Let's review:
# MAGIC
# MAGIC ✅ Question 1: Correct - C
# MAGIC ✅ Question 2: Correct - A
# MAGIC ✅ Question 3: Correct - D
# MAGIC ❌ Question 4: You answered B, but the correct answer is C
# MAGIC ✅ Question 5: Correct - B
# MAGIC
# MAGIC For Question 4, the key is understanding that VACUUM removes files older than the retention period, not younger. Want me to explain this concept more?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **You**: Yes please, explain the VACUUM retention thing again
# MAGIC
# MAGIC **AI**: Key concept:
# MAGIC
# MAGIC VACUUM removes old data files that are no longer needed, BUT it keeps files from the last X hours to enable time travel.
# MAGIC
# MAGIC Default retention: 7 days (168 hours)
# MAGIC
# MAGIC So `VACUUM table_name` will:
# MAGIC - ✅ Remove files older than 7 days
# MAGIC - ❌ Keep files from the last 7 days
# MAGIC
# MAGIC If you want to remove files sooner, you need to:
# MAGIC 1. Disable the safety check: `SET spark.databricks.delta.retentionDurationCheck.enabled = false`
# MAGIC 2. Specify a shorter period: `VACUUM table_name RETAIN 24 HOURS`
# MAGIC
# MAGIC ⚠️ **Exam Trap**: Questions often ask what happens with RETAIN 0 HOURS - this removes ALL old files immediately, breaking time travel!
# MAGIC
# MAGIC This addresses the conceptual gap.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **You**: I think I'm ready to move to Topic 4
# MAGIC
# MAGIC **AI**: Before proceeding, verify retention:
# MAGIC
# MAGIC - [ ] Can you write a MERGE statement from memory?
# MAGIC - [ ] Do you know the difference between OPTIMIZE and VACUUM?
# MAGIC - [ ] Can you explain when to use ZORDER BY?
# MAGIC - [ ] Do you understand VACUUM retention?
# MAGIC
# MAGIC If confident on all items, proceed to Topic 4 (Incremental Data Processing). That topic covers streaming, which requires more attention given your limited streaming experience.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Working Together
# MAGIC
# MAGIC Workflow: Study → Practice → Request assistance → Apply feedback → Repeat
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Ready to Start?
# MAGIC %md
# MAGIC ## Getting Started
# MAGIC
# MAGIC ### Your Next Steps:
# MAGIC
# MAGIC 1. **Open** [01_study_plan.py](#notebook-4024127848180323)
# MAGIC    - Review the complete study roadmap
# MAGIC    - Note your personalized focus areas
# MAGIC    - Check the 6-week schedule
# MAGIC
# MAGIC 2. **Start with Topic 1** (or jump to your weakest area)
# MAGIC    - Open `01_Databricks_Lakehouse_Platform/overview.py`
# MAGIC    - Read through all concepts
# MAGIC    - Run the code examples
# MAGIC
# MAGIC 3. **Practice**
# MAGIC    - Open `01_Databricks_Lakehouse_Platform/practice_tasks.py`
# MAGIC    - Run setup, attempt exercises
# MAGIC    - Ask me for help anytime!
# MAGIC
# MAGIC 4. **Check Solutions**
# MAGIC    - Open `01_Databricks_Lakehouse_Platform/solutions.py`
# MAGIC    - Compare your work
# MAGIC    - Learn from explanations
# MAGIC
# MAGIC 5. **Track Progress**
# MAGIC    - Use checkboxes in Master Plan
# MAGIC    - Note weak areas
# MAGIC    - Review regularly
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Requesting Assistance
# MAGIC
# MAGIC Request assistance using natural language. Examples:
# MAGIC
# MAGIC **Checking Work**:
# MAGIC - "Check my solution for Exercise 3 in Topic 2"
# MAGIC - "Is my MERGE statement correct?"
# MAGIC - "Does my output match the expected result?"
# MAGIC
# MAGIC **Understanding Concepts**:
# MAGIC - "Explain OPTIMIZE vs VACUUM again"
# MAGIC - "I don't understand checkpointing"
# MAGIC - "Why is this config important?"
# MAGIC
# MAGIC **Getting Unstuck**:
# MAGIC - "Hint for Exercise 8 please"
# MAGIC - "Why isn't my streaming query working?"
# MAGIC - "What concept should I use here?"
# MAGIC
# MAGIC **Practice & Review**:
# MAGIC - "Quiz me on Delta Lake syntax"
# MAGIC - "Test my understanding of Spark configs"
# MAGIC - "Am I ready for the exam?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - All required materials are provided in this structure
# MAGIC - Assistance is available throughout the study process
# MAGIC - Productive struggle improves long-term retention
# MAGIC - The exam validates existing knowledge applied to Databricks context
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Begin when ready
# MAGIC
# MAGIC To start: "I'm ready to begin Topic 1" or **"Let's start with [topic name]"**
