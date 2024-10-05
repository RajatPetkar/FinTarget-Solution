const express = require('express');
const fs = require('fs');
const app = express();

app.use(express.json());

const userTaskQueues = {};  // Stores task queues for each user
const rateLimitData = {};   // Stores rate limit info for each user

// Rate limiting settings
const maxTasksPerSecond = 1;
const maxTasksPerMinute = 20;

// Function to log task completion
async function logTaskCompletion(user_id, timestamp) {
  const log = `${user_id}-task completed at-${timestamp}\n`;
  fs.appendFileSync('task_log.txt', log);
  console.log(log);  // Also log to console
}

// Function to process queued tasks for a specific user
async function processNextTask(user_id) {
  const userQueue = userTaskQueues[user_id];
  if (userQueue.length > 0) {
    const { task, timestamp } = userQueue.shift();
    await logTaskCompletion(user_id, timestamp);

    // Process the next task after 1 second if more tasks are in the queue
    if (userQueue.length > 0) {
      setTimeout(() => processNextTask(user_id), 1000);
    }
  }
}

// Function to add task to queue
function addTaskToQueue(user_id, task) {
  if (!userTaskQueues[user_id]) {
    userTaskQueues[user_id] = [];
  }
  userTaskQueues[user_id].push(task);
  
  // If the queue is empty or not processing, start processing
  if (userTaskQueues[user_id].length === 1) {
    setTimeout(() => processNextTask(user_id), 1000);
  }
}

// Middleware for rate limiting and queuing
function rateLimiter(req, res, next) {
  const user_id = req.body.user_id;
  const currentTime = Date.now();

  if (!rateLimitData[user_id]) {
    rateLimitData[user_id] = {
      lastTaskTime: 0,
      taskCountInMinute: 0,
    };
  }

  const userData = rateLimitData[user_id];
  const timeSinceLastTask = currentTime - userData.lastTaskTime;

  // Reset the task count every minute
  if (currentTime - userData.lastTaskTime >= 60000) {
    userData.taskCountInMinute = 0;
  }

  // Enforce the per-minute rate limit
  if (userData.taskCountInMinute >= maxTasksPerMinute) {
    return res.status(429).send('Rate limit exceeded: Too many tasks per minute');
  }

  // Enforce the per-second rate limit
  if (timeSinceLastTask < 1000) {
    addTaskToQueue(user_id, { task: req.body, timestamp: currentTime });
    return res.status(202).send('Task queued due to rate limit');
  }

  // Update rate limiting data
  userData.lastTaskTime = currentTime;
  userData.taskCountInMinute++;

  next();
}

app.post('/task', rateLimiter, (req, res) => {
  const { user_id } = req.body;
  const currentTime = Date.now();

  // Process the task immediately (logs the task)
  addTaskToQueue(user_id, { task: req.body, timestamp: currentTime });

  res.send('Task added to queue');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
