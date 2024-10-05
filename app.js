const express = require('express');
const fs = require('fs');
const app = express();

app.use(express.json());

const userTaskQueues = {}; 
const rateLimitData = {};  

const maxTasksPerSecond = 1;
const maxTasksPerMinute = 20;

async function logTaskCompletion(user_id, timestamp) {
  const log = `${user_id}-task completed at-${timestamp}\n`;
  fs.appendFileSync('task_log.txt', log);
  console.log(log);  
}

async function processNextTask(user_id) {
  const userQueue = userTaskQueues[user_id];
  if (userQueue.length > 0) {
    const { task, timestamp } = userQueue.shift();
    await logTaskCompletion(user_id, timestamp);

    if (userQueue.length > 0) {
      setTimeout(() => processNextTask(user_id), 1000);
    }
  }
}

function addTaskToQueue(user_id, task) {
  if (!userTaskQueues[user_id]) {
    userTaskQueues[user_id] = [];
  }
  userTaskQueues[user_id].push(task);
  
  if (userTaskQueues[user_id].length === 1) {
    setTimeout(() => processNextTask(user_id), 1000);
  }
}

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

  if (currentTime - userData.lastTaskTime >= 60000) {
    userData.taskCountInMinute = 0;
  }

  if (userData.taskCountInMinute >= maxTasksPerMinute) {
    return res.status(429).send('Rate limit exceeded: Too many tasks per minute');
  }

  if (timeSinceLastTask < 1000) {
    addTaskToQueue(user_id, { task: req.body, timestamp: currentTime });
    return res.status(202).send('Task queued due to rate limit');
  }

  userData.lastTaskTime = currentTime;
  userData.taskCountInMinute++;

  next();
}

app.post('/task', rateLimiter, (req, res) => {
  const { user_id } = req.body;
  const currentTime = Date.now();

  addTaskToQueue(user_id, { task: req.body, timestamp: currentTime });

  res.send('Task added to queue');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
