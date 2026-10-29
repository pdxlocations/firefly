// Run with: node --test tests/test_message_snapshots.js
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require('node:path').join(__dirname, '../templates/index.html'), 'utf8');
const functions = ['upsertMessage', 'beginMessageSnapshot', 'mergeMessageSnapshot', 'applyMessageSnapshot', 'loadMessages', 'refreshThreadStateSnapshot']
    .map(name => source.match(new RegExp(`(?:async )?function ${name}\\([^]*?\\n\\}`))[0]).join('\n');

function fixture() {
    const requests = [];
    const context = {
        currentProfile: { id: 'p' }, activeChannelIndex: 0,
        channelMessages: [], dmMessages: [], messageSnapshotRequestId: 0,
        console: { error() {} },
        fetch: () => new Promise(resolve => requests.push(payload => resolve({ ok: true, json: async () => payload }))),
        reconcileMessageSenderLabelsFromNodes() {}, syncDmTargetsFromMessages() {}, applyUnreadState() {},
        loadDmTargets: async () => {}, renderActiveMessages() {}, updateComposerState() {},
    };
    vm.createContext(context);
    vm.runInContext(functions, context);
    return { context, requests };
}

for (const method of ['loadMessages', 'refreshThreadStateSnapshot']) {
    test(`${method} keeps channel and DM events arriving during a history request`, async () => {
        const { context: c, requests } = fixture();
        const pending = c[method]();
        c.upsertMessage(c.channelMessages, { id: 'channel-live', content: 'hello' });
        c.upsertMessage(c.dmMessages, { id: 'dm-live', content: 'private' });
        requests[0]({ channel_messages: [], dm_messages: [] });
        await pending;
        assert.equal(c.channelMessages[0].id, 'channel-live');
        assert.equal(c.dmMessages[0].id, 'dm-live');
    });
}

test('stale history does not overwrite a live ACK update or duplicate the message', async () => {
    const { context: c, requests } = fixture();
    c.dmMessages = [{ id: 'dm', ack_status: 'pending' }];
    const pending = c.loadMessages();
    c.upsertMessage(c.dmMessages, { id: 'dm', ack_status: 'ack' });
    requests[0]({ dm_messages: [{ id: 'dm', ack_status: 'pending' }] });
    await pending;
    assert.equal(c.dmMessages.length, 1);
    assert.equal(c.dmMessages[0].ack_status, 'ack');
});

test('out-of-order responses cannot replace newer history', async () => {
    const { context: c, requests } = fixture();
    const first = c.loadMessages();
    const second = c.refreshThreadStateSnapshot();
    requests[1]({ channel_messages: [{ id: 'newer' }] });
    await second;
    requests[0]({ channel_messages: [{ id: 'older' }] });
    await first;
    assert.equal(c.channelMessages[0].id, 'newer');
});

test('a response for a previous profile is ignored', async () => {
    const { context: c, requests } = fixture();
    const pending = c.loadMessages();
    c.currentProfile = { id: 'other' };
    requests[0]({ channel_messages: [{ id: 'wrong-profile' }] });
    await pending;
    assert.equal(c.channelMessages.length, 0);
});

test('history still removes old records when no live update arrived', async () => {
    const { context: c, requests } = fixture();
    c.channelMessages = [{ id: 'deleted' }];
    const pending = c.loadMessages();
    requests[0]({ channel_messages: [] });
    await pending;
    assert.equal(c.channelMessages.length, 0);
});
