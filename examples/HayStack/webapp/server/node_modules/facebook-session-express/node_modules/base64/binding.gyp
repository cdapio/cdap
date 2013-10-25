{
    "targets": [
        {
            "target_name": "base64",
            "sources": [
                "base64.cc"
            ]
        },
        {
            "target_name": "after_build",
            "type": "none",
            "dependencies": [
                "base64"
            ],
            "actions": [
                {
                    "action_name": "symlink",
                    "inputs": [
                        "<@(PRODUCT_DIR)/base64.node"
                    ],
                    "outputs": [
                        "<(module_root_dir)/base64.node"
                    ],
                    "action": ["ln", "-s", "<@(PRODUCT_DIR)/base64.node", "<(module_root_dir)/base64.node"]
                }
            ]
        }
    ]
}
