// Run with: node --test tests/test_message_resend.js
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const source = fs.readFileSync(require('node:path').join(__dirname, '../templates/index.html'), 'utf8');
const names = ['formatAckReason', 'getAckStatusMeta', 'renderAckIconSvg', 'renderAckStatusHtml', 'resendMessage'];
const functions = names.map(name => {
    const match = source.match(new RegExp(`(?:async )?function ${name}\\([^]*?\\n\\}`));
    assert.ok(match, name);
    return match[0];
}).join('\n');

function fixture() {
    const message = {
        id: 'failed-message', direction: 'sent', message_type: 'dm',
        owner_profile_id: 'profile', sender: '!12345678', ack_requested: true,
        ack_status: 'max_retransmit', target_node_num: 1308714549,
        content: 'original text', reply_packet_id: 123,
    };
    const requests = [], alerts = [];
    const context = {
        currentProfile: { id: 'profile', node_id: '!12345678' },
        dmMessages: [message], resendingMessageIds: new Set(),
        escapeHtml: value => String(value),
        document: { querySelector: () => ({ content: 'csrf' }), querySelectorAll: () => [] },
        fetch: async (url, options) => {
            requests.push({ url, options });
            return { ok: true, json: async () => ({}) };
        },
        loadMessages: async () => {}, loadDmTargets: async () => {}, renderActiveMessages: () => {},
        alert: text => alerts.push(text),
    };
    vm.createContext(context);
    vm.runInContext(functions, context);
    return { context, message, requests, alerts };
}

test('resend appears only beside failed outgoing DM statuses', () => {
    const { context, message } = fixture();
    for (const status of ['max_retransmit', 'nak', 'pending', 'ack', 'implicit_ack']) {
        message.ack_status = status;
        assert.equal(context.renderAckStatusHtml(message).includes('data-resend-message-id'),
            ['max_retransmit', 'nak'].includes(status));
    }
    message.ack_status = 'max_retransmit';
    message.owner_profile_id = 'another-profile';
    assert.equal(context.renderAckStatusHtml(message).includes('data-resend-message-id'), false);
});

test('resend preserves original text, recipient and reply; blocks duplicate taps', async () => {
    const { context, requests } = fixture();
    const button = {};
    const first = context.resendMessage('failed-message', button);
    assert.equal(button.disabled, true);
    await context.resendMessage('failed-message', button);
    await first;
    assert.equal(requests.length, 1);
    assert.deepEqual(JSON.parse(requests[0].options.body), {
        message: 'original text', message_type: 'dm', target_node_num: 1308714549, reply_packet_id: 123,
    });
    assert.equal(requests[0].options.headers['X-CSRF-Token'], 'csrf');
    assert.equal(button.disabled, false);
});

test('failed request restores resend control and displays error', async () => {
    const { context, alerts } = fixture();
    context.fetch = async () => ({ ok: false, json: async () => ({ error: 'Missing key' }) });
    const button = {};
    await context.resendMessage('failed-message', button);
    assert.equal(button.disabled, false);
    assert.equal(context.resendingMessageIds.size, 0);
    assert.match(alerts[0], /Missing key/);
});

test('already acknowledged messages cannot be resent', async () => {
    const { context, message, requests } = fixture();
    message.ack_status = 'ack';
    await context.resendMessage(message.id, {});
    assert.equal(requests.length, 0);
});
