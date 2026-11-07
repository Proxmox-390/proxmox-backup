Ext.define('PBS.DataStoreNotes', {
    extend: 'Proxmox.panel.NotesView',
    xtype: 'pbsDataStoreNotes',

    title: gettext('Comment'),
    maxLength: 8 * 1024,

    collapsible: true,
    collapseDirection: 'right',

    cbindData: function (initialConfig) {
        let me = this;
        me.url = `/api2/extjs/config/datastore/${me.datastore}`;
        return {};
    },

    // The editor and loading of the toolkit's view are fixed to the 'description' property.
    run_editor: function () {
        let me = this;
        Ext.create('Proxmox.window.NotesEdit', {
            title: me.title,
            url: me.url,
            onlineHelp: me.onlineHelp,
            items: {
                xtype: 'textarea',
                name: 'comment',
                height: '100%',
                value: '',
                maxLength: me.maxLength,
                hideLabel: true,
                emptyText: gettext('You can use Markdown for rich text formatting.'),
                fieldStyle: {
                    'white-space': 'pre-wrap',
                    'font-family': 'monospace',
                },
            },
            listeners: {
                destroy: () => me.load(),
            },
            autoShow: true,
        });
    },

    load: function () {
        let me = this;

        Proxmox.Utils.API2Request({
            url: me.url,
            waitMsgTarget: me,
            failure: (response) => {
                me.update(gettext('Error') + ' ' + response.htmlStatus);
                me.setCollapsed(false);
            },
            success: ({ result }) => me.setNotes(result.data.comment),
        });
    },
});
