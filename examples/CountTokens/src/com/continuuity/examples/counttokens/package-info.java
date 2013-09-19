/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * Package for the CountTokens sample application.
 * 
 * Reads events (= byte[] body, Map<String,String> headers) from the 'text' input stream.
 * 
 * Tokenizes the text in the body and in the header named 'title', ignores all other headers.
 * 
 * Each token is cloned into two tokens:
 *   a) the upper case version of the token
 *   b) the original token with a field prefix 'title',
 *   or 'text' if the token is from the body of the event.
 *
 * All of the cloned tokens are counted using increment operations.
 */
package com.continuuity.examples.counttokens;


