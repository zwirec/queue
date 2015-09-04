local state = require 'queue.abstract.state'
local tube = {}
local method = {}
local log = require 'log'
local json = require 'json'
local fiber = require 'fiber'

local TIMEOUT_INFINITY  = 365 * 86400 * 500
local LIMIT_DEFAULT     = 1

local i_id              = 1
local i_status          = 2
local i_next_event      = 3
local i_ttl             = 4
local i_ttr             = 5
local i_pri             = 6
local i_created         = 7
local i_utube           = 8
local i_data            = 9


local function time(tm)
    if tm == nil then
        tm = fiber.time()
    end
    return 0ULL + (tm * 1000000)
end

local function event_time(timeout)
    if timeout == nil then
        box.error(box.error.PROC_LUA, debug.traceback())
    end
    return 0ULL + ((fiber.time() + timeout) * 1000000)
end

-- create space
function tube.create_space(space_name, opts)
    if opts.ttl == nil then
        opts.ttl = TIMEOUT_INFINITY
    end

    if opts.ttr == nil then
        opts.ttr = opts.ttl
    end

    if opts.pri == nil then
        opts.pri = 0
    end

    local space_opts = {}
    space_opts.temporary = opts.temporary

    -- 1        2       3           4    5    6    7,       8
    -- task_id, status, next_event, ttl, ttr, pri, created, data
    local space = box.schema.create_space(space_name, space_opts)

    space:create_index('task_id', { type = 'tree', parts = { i_id, 'num' }})
    space:create_index('status',
        { type = 'tree',
            parts = { i_status, 'str', i_pri, 'num', i_id, 'num' }})
    space:create_index('watch',
        { type = 'tree', parts = { i_status, 'str', i_next_event, 'num' },
            unique = false})
    space:create_index('utube',
        { type = 'tree',
            parts = { i_status, 'str', i_utube, 'str', i_id, 'num' }})
    return space
end


-- start tube on space
function tube.new(space, on_task_change, opts)
    if on_task_change == nil then
        on_task_change = function() end
    end
    if opts.limit == nil then
        opts.limit = {}
    elseif type(opts.limit) == 'table' then
        for utube, limit in pairs(opts.limit) do
            if limit < 1 then
                error(string.format("Invalid limit %s for %s", limit, utube))
            end
        end
    else
        error('invalid opts.limit, expected table[utube] = limit')
    end
    local self = {
        space           = space,
        on_task_change  = function(self, task, stat_data)
            -- wakeup fiber
            if task ~= nil then
                if self.fiber ~= nil then
                    if self.fiber:id() ~= fiber.id() then
                        self.fiber:wakeup()
                    end
                end
            end
            on_task_change(task, stat_data)
        end,
        opts            = opts,
    }
    setmetatable(self, { __index = method })

    self.fiber = fiber.create(self._fiber, self)

    return self
end

-- Upgrade one BLOCKED task to READY status
local function unblock_one(self, utube)
    local neighbour = self.space.index.utube:min{state.BLOCKED, utube}
    if neighbour == nil or neighbour[i_utube] ~= utube or
        neighbour[i_status] ~= state.BLOCKED then
            return
    end
    neighbour = self.space:update(neighbour[i_id],
        { { '=', i_status, state.READY } })
    self:on_task_change(neighbour)
end

-- Check if a new task should be in READY or BLOCKED state
local function check_limit(self, utube)
    -- space contains at most one TAKEN or READY tuple
    local limit = self.opts.limit[utube] or LIMIT_DEFAULT
    assert(limit >= 1)
    local cnt = self.space.index.utube:count({state.TAKEN, utube})
    if cnt < limit then
        cnt = cnt + self.space.index.utube:count({state.READY, utube})
    end
    if cnt < limit then
        return state.READY
    end
    return state.BLOCKED
end

-- watch fiber
function method._fiber(self)
    fiber.name('utubettl')
    log.info("Started queue utubettl fiber")
    local estimated
    local ttl_statuses = { state.READY, state.BLOCKED, state.BURIED }
    local now, task

    while true do
        estimated = TIMEOUT_INFINITY
        now = time()

        -- delayed tasks
        task = self.space.index.watch:min{ state.DELAYED }
        if task and task[i_status] == state.DELAYED then
            if now >= task[i_next_event] then
                task = self.space:update(task[i_id], {
                    { '=', i_status, check_limit(self, task[i_utube]) },
                    { '=', i_next_event, task[i_created] + task[i_ttl] }
                })
                self:on_task_change(task)
                estimated = 0
            else
                estimated = tonumber(task[i_next_event] - now) / 1000000
            end
        end

        -- ttl tasks
        for _, state in pairs(ttl_statuses) do
            task = self.space.index.watch:min{ state }
            if task ~= nil and task[i_status] == state then
                if now >= task[i_next_event] then
                    self.space:delete(task[i_id])
                    task = task:update{{'=', i_status, state.DONE}}
                    self:on_task_change(task)
                    estimated = 0
                else
                    local et = tonumber(task[i_next_event] - now) / 1000000
                    if et < estimated then
                        estimated = et
                    end
                end
            end
        end

        -- ttr tasks
        task = self.space.index.watch:min{ state.TAKEN }
        if task and task[i_status] == state.TAKEN then
            if now >= task[i_next_event] then
                -- READY <-> TAKEN doesn't affect utube limit invariant
                task = self.space:update(task[i_id], {
                    { '=', i_status, state.READY },
                    { '=', i_next_event, task[i_created] + task[i_ttl] }
                })
                self:on_task_change(task)
                estimated = 0
            else
                local et = tonumber(task[i_next_event] - now) / 1000000
                if et < estimated then
                    estimated = et
                end
            end
        end


        if estimated > 0 then
            -- free refcounter
            task = nil
            fiber.sleep(estimated)
        end
    end
end


-- cleanup internal fields in task
function method.normalize_task(self, task)
    if task ~= nil then
        return task:transform(i_next_event, i_data - i_next_event)
    end
end

-- put task in space
function method.put(self, data, opts)
    local max = self.space.index.task_id:max()
    local id = 0
    if max ~= nil then
        id = max[i_id] + 1
    end

    local status
    local ttl = opts.ttl or self.opts.ttl
    local ttr = opts.ttr or self.opts.ttr
    local pri = opts.pri or self.opts.pri or 0
    local utube = opts.utube and tostring(opts.utube) or ""

    local next_event

    if opts.delay ~= nil and opts.delay > 0 then
        status = state.DELAYED
        ttl = ttl + opts.delay
        next_event = event_time(opts.delay)
    else
        status = check_limit(self, utube)
        next_event = event_time(ttl)
    end

    local task = self.space:insert{
            id,
            status,
            next_event,
            time(ttl),
            time(ttr),
            pri,
            time(),
            utube,
            data
    }
    self:on_task_change(task, 'put')
    return task
end


-- take task
function method.take(self)
    -- READY <-> TAKEN doesn't affect utube limit invariant
    local task = self.space.index.status:min{state.READY}
    if task ~= nil and task[i_status] == state.READY then
        local next_event = time() + task[i_ttr]
        task = self.space:update(task[1], {
            { '=', i_status, state.TAKEN },
            { '=', i_next_event, next_event },
        })
        self:on_task_change(task, 'take')
        return task
    end
end


-- delete task
function method.delete(self, id)
    local task = self.space:delete(id)
    if task == nil then
        return
    end
    local new_task = task:update({ {'=', i_status, state.DONE} })
    self:on_task_change(new_task, 'delete')
    -- utube limit: unblock the next task if status was READY or TAKEN
    if task[i_status] == state.READY or task[i_status] == state.TAKEN then
        unblock_one(self, task[i_utube])
    end
    return new_task
end

-- release task
function method.release(self, id, opts)
    local task = self.space:get{id}
    if task == nil then
        return
    end
    local new_task
    if opts.delay ~= nil and opts.delay > 0 then
        new_task = self.space:update(id, {
            { '=', i_status, state.DELAYED },
            { '=', i_next_event, event_time(opts.delay) },
            { '+', i_ttl, opts.delay }
        })
    else
        new_task = self.space:update(id, {
            { '=', i_status, state.BLOCKED },
            { '=', i_next_event, task[i_created] + task[i_ttl] }
        })
    end
    self:on_task_change(new_task, 'release')
    -- utube limit: unblock the next task if status was READY or TAKEN
    if task[i_status] == state.READY or task[i_status] == state.TAKEN then
        unblock_one(self, task[i_utube])
    end
    return new_task
end

-- bury task
function method.bury(self, id)
    local task = self.space:get{id}
    if task == nil then
        return
    end
    local new_task = self.space:update(id, {{ '=', i_status, state.BURIED }})
    self:on_task_change(new_task, 'bury')
    -- utube limit: unblock the next task if status was READY or TAKEN
    if task[i_status] == state.READY or task[i_status] == state.TAKEN then
        unblock_one(self, task[i_utube])
    end
    return new_task
end

-- unbury several tasks
function method.kick(self, count)
    for i = 1, count do
        local task = self.space.index.status:min{ state.BURIED }
        if task == nil or task[i_status] ~= state.BURIED then
            return i - 1
        end

        task = self.space:update(task[i_id], {{ '=', i_status,
            check_limit(self, task[i_utube]) }})
        self:on_task_change(task, 'kick')
    end
    return count
end

-- peek task
function method.peek(self, id)
    return self.space:get{id}
end


return tube
